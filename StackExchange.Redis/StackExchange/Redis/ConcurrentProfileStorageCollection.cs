using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    /// <summary>
    /// A collection of IProfiledCommands.
    /// 
    /// This is a very light weight data struction, only supporting enumeration.
    /// 
    /// While it implements IEnumerable, it there are fewer allocations if one uses
    /// it's explicit GetEnumerator() method.  Using `foreach` does this automatically.
    /// </summary>
    public struct ProfiledCommandEnumerable : IEnumerable<IProfiledCommand>
    {
        /// <summary>
        /// Implements IEnumerator for ProfiledCommandEnumerable.
        /// This implementation is comparable to List.Enumerator and Dictionary.Enumerator,
        /// and is provided to reduce allocations in the common (ie. foreach) case.
        /// </summary>
        public struct Enumerator : IEnumerator<IProfiledCommand>
        {
            ProfileStorage Head;
            bool BeforeFirstElement;
            ProfileStorage CurrentBacker;
            
            internal Enumerator(ProfileStorage head)
            {
                Head = head;
                BeforeFirstElement = true;
                CurrentBacker = null;
            }

            /// <summary>
            /// The current element.
            /// </summary>
            public IProfiledCommand Current
            {
                get { return CurrentBacker; }
            }

            object System.Collections.IEnumerator.Current 
            {
                get
                {
                    return CurrentBacker;
                }
            }

            /// <summary>
            /// Advances the enumeration, returning true if there is a new element to consume and false
            /// if enumeration is complete.
            /// </summary>
            public bool MoveNext()
            {
                if (BeforeFirstElement)
                {
                    CurrentBacker = Head;
                    BeforeFirstElement = false;
                }
                else
                {
                    CurrentBacker = CurrentBacker.NextElement;
                }

                return Current != null;
            }

            /// <summary>
            /// Resets the enumeration.
            /// </summary>
            public void Reset()
            {
                CurrentBacker = null;
                BeforeFirstElement = true;
            }

            /// <summary>
            /// Disposes the enumeration.
            /// subsequent attempts to enumerate results in undefined behavior.
            /// </summary>
            public void Dispose()
            {
                CurrentBacker = Head = null;
            }
        }

        ProfileStorage Head;

        internal ProfiledCommandEnumerable(ProfileStorage head)
        {
            Head = head;
        }

        /// <summary>
        /// Returns an implementor of IEnumerator that, provided it isn't accessed
        /// though an interface, avoids allocations.
        /// 
        /// `foreach` will automatically use this method.
        /// </summary>
        public Enumerator GetEnumerator()
        {
            return new Enumerator(Head);
        }

        IEnumerator<IProfiledCommand> IEnumerable<IProfiledCommand>.GetEnumerator()
        {
            return GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    /// <summary>
    /// A thread-safe collection tailored to the "always append, with high contention, then enumerate once with no contention"
    /// behavior of our profiling.
    /// 
    /// Performs better than ConcurrentBag, which is important since profiling code shouldn't impact timings.
    /// </summary>
    sealed class ConcurrentProfileStorageCollection
    {
        // internal for test purposes
        internal static int AllocationCount = 0;

        // It is, by definition, impossible for an element to be in 2 intrusive collections
        //   and we force Enumeration to release any reference to the collection object
        //   so we can **always** pool these.
        const int PoolSize = 64;
        static ConcurrentProfileStorageCollection[] Pool = new ConcurrentProfileStorageCollection[PoolSize];

        volatile ProfileStorage Head;

        private ConcurrentProfileStorageCollection() { }

        /// <summary>
        /// This method is thread-safe.
        /// 
        /// Adds an element to the bag.
        /// 
        /// Order is not preserved.
        /// 
        /// The element can only be a member of *one* bag.
        /// </summary>
        public void Add(ProfileStorage command)
        {
            do
            {
                var cur = Head;
                command.NextElement = cur;

                // Interlocked references to volatile fields are perfectly cromulent
#pragma warning disable 420
                var got = Interlocked.CompareExchange(ref Head, command, cur);
#pragma warning restore 420

                if (object.ReferenceEquals(got, cur)) break;
            } while (true);
        }

        /// <summary>
        /// This method returns an enumerable view of the bag.
        /// 
        /// It is not thread safe.
        /// 
        /// It should only be called once the bag is finished being mutated.
        /// </summary>
        public ProfiledCommandEnumerable EnumerateAndReturnForReuse()
        {
            var ret = new ProfiledCommandEnumerable(Head);

            // no need for interlocking, this isn't a thread safe method
            Head = null;

            for (var i = 0; i < PoolSize; i++)
            {
                if (Interlocked.CompareExchange(ref Pool[i], this, null) == null) break;
            }

            return ret;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static ConcurrentProfileStorageCollection GetOrCreate()
        {
            ConcurrentProfileStorageCollection found;
            for (int i = 0; i < PoolSize; i++)
            {
                if ((found = Interlocked.Exchange(ref Pool[i], null)) != null)
                {
                    return found;
                }
            }

            Interlocked.Increment(ref AllocationCount);
            found = new ConcurrentProfileStorageCollection();

            return found;
        }
    }
}
