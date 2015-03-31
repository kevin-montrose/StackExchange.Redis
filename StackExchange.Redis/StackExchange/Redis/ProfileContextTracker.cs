using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    /// <summary>
    /// Big ol' wrapper around most of the profiling storage logic, 'cause it got too big to just live in ConnectionMultiplexer.
    /// </summary>
    class ProfileContextTracker
    {
        /// <summary>
        /// Necessary, because WeakReference can't be readily comparable (since the reference is... weak).
        /// 
        /// This lets us detect leaks* with some reasonable confidence, and cleanup periodically.
        /// 
        /// Some callestenics are done to avoid allocating WeakReferences for no reason, as often
        /// we're just looking up ProfileStorage.
        /// 
        /// * Somebody starts profiling, but for whatever reason never *stops* with a context object
        /// </summary>
        struct ProfileContextCell : IEquatable<ProfileContextCell>
        {
            object HardReference;
            WeakReference<object> WeakReference;
            int HashCode;

            public bool IsContextLeaked
            {
                get
                {
                    object ignored;
                    return !TryGetTarget(out ignored);
                }
            }

            private ProfileContextCell(object forObj, bool isEphemeral)
            {
                HashCode = forObj.GetHashCode();

                if (isEphemeral)
                {
                    HardReference = forObj;
                    WeakReference = null;
                }
                else
                {
                    HardReference = null;
                    WeakReference = new WeakReference<object>(forObj, trackResurrection: true); // ughhh, have to handle finalizers
                }
            }

            public static ProfileContextCell ToStoreUnder(object forObj)
            {
                return new ProfileContextCell(forObj, isEphemeral: false);
            }

            public static ProfileContextCell ToLookupBy(object forObj)
            {
                return new ProfileContextCell(forObj, isEphemeral: true);
            }

            bool TryGetTarget(out object target)
            {
                if (HardReference != null)
                {
                    target = HardReference;
                    return true;
                }

                return WeakReference.TryGetTarget(out target);
            }

            public override bool Equals(object obj)
            {
                if (!(obj is ProfileContextCell)) return false;

                return Equals((ProfileContextCell)obj);
            }

            public override int GetHashCode()
            {
                return HashCode;
            }

            public bool Equals(ProfileContextCell other)
            {
                object thisObj, otherObj;

                if (other.TryGetTarget(out otherObj) != TryGetTarget(out thisObj)) return false;

                // dead references are equal
                if (thisObj == null) return true;

                return thisObj.Equals(otherObj);
            }
        }

        // provided so default behavior doesn't do any boxing, for sure
        class ProfileContextCellComparer : IEqualityComparer<ProfileContextCell>
        {
            public static readonly ProfileContextCellComparer Singleton = new ProfileContextCellComparer();

            private ProfileContextCellComparer() { }

            public bool Equals(ProfileContextCell x, ProfileContextCell y)
            {
                return x.Equals(y);
            }

            public int GetHashCode(ProfileContextCell obj)
            {
                return obj.GetHashCode();
            }
        }

        private long lastCleanupSweep;
        private ConcurrentDictionary<ProfileContextCell, ConcurrentIntrusiveCollection<ProfileStorage>> profiledCommands;

        public int ContextCount { get { return profiledCommands.Count;  } }

        public ProfileContextTracker()
        {
            profiledCommands = new ConcurrentDictionary<ProfileContextCell, ConcurrentIntrusiveCollection<ProfileStorage>>(ProfileContextCellComparer.Singleton);
            lastCleanupSweep = DateTime.UtcNow.Ticks;
        }

        public bool TryCreate(object ctx)
        {
            var cell = ProfileContextCell.ToStoreUnder(ctx);
            return profiledCommands.TryAdd(cell, new ConcurrentIntrusiveCollection<ProfileStorage>());
        }

        public bool TryGetValue(object ctx, out ConcurrentIntrusiveCollection<ProfileStorage> val)
        {
            var cell = ProfileContextCell.ToLookupBy(ctx);
            return profiledCommands.TryGetValue(cell, out val);
        }

        public bool TryRemove(object ctx, out IEnumerable<IProfiledCommand> commands)
        {
            var cell = ProfileContextCell.ToLookupBy(ctx);
            ConcurrentIntrusiveCollection<ProfileStorage> storage;
            if (!profiledCommands.TryRemove(cell, out storage))
            {
                commands = null;
                return false;
            }

            commands = storage.Enumerate();
            return true;
        }

        public bool TryCleanup()
        {
            const long SweepEveryTicks = 600000000; // once a minute, tops

            var now = DateTime.UtcNow.Ticks;    // resolution on this isn't great, but it's good enough
            var last = lastCleanupSweep;
            var since = now - last;
            if (since < SweepEveryTicks) return false;

            // this is just to keep other threads from wasting time, in theory
            //  it'd be perfectly safe for this to run concurrently
            var saw = Interlocked.CompareExchange(ref lastCleanupSweep, now, last);
            if (saw != last) return false;

            var allDead = profiledCommands.Keys.Where(k => k.IsContextLeaked).ToList();
            foreach (var dead in allDead)
            {
                ConcurrentIntrusiveCollection<ProfileStorage> ignored;
                profiledCommands.TryRemove(dead, out ignored);
            }

            return true;
        }
    }
}
