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
    /// A thread-safe collection tailored to the "always append, with high contention, then enumerate once with no contention"
    /// behavior of our profiling.
    /// 
    /// Performs better than ConcurrentBag, which is important since profiling code shouldn't impact timings.
    /// </summary>
    sealed class ConcurrentAddOnlyBag<T>
        where T : INextElement<T>
    {
        volatile INextElement<T> Head;

        public ConcurrentAddOnlyBag() { }

        /// <summary>
        /// This method is thread-safe.
        /// 
        /// Adds an element to the bag.
        /// 
        /// Order is not preserved.
        /// 
        /// The element can only be a member of *one* bag.
        /// </summary>
        public void Add(T command)
        {
            while(true)
            {
                var cur = Head;
                command.NextElement = cur;

                // Interlocked references to voliatle fields are perfectly cromulent
#pragma warning disable 420
                var got = Interlocked.CompareExchange(ref Head, command, cur);
#pragma warning restore 420

                if (object.ReferenceEquals(got, cur)) break;
            }
        }

        /// <summary>
        /// This method returns an enumerable view of the Bag.
        /// 
        /// It is not thread safe.  It should only be called once the bag is finished being mutated.
        /// </summary>
        public IEnumerable<T> Enumerate()
        {
            var cur = Head;
            while(cur != null)
            {
                yield return cur.Value;
                cur = cur.NextElement;
            }
        }
    }

    interface INextElement<Self>
        where Self : INextElement<Self>
    {
        INextElement<Self> NextElement { get; set; }
        Self Value { get; }
    }

    partial class ConnectionMultiplexer
    {
        private IProfiler profiler;
        private ConcurrentDictionary<object, ConcurrentAddOnlyBag<ProfileStorage>> profiledCommands;
        /// <summary>
        /// Sets an IProfiler instance for this ConnectionMultiplexer.
        /// 
        /// An IProfiler instances is used to determine which context to associate an
        /// IProfiledCommand with.  See BeginProfiling(object) and FinishProfiling(object)
        /// for more details.
        /// </summary>
        public void RegisterProfiler(IProfiler profiler)
        {
            if (profiler == null) throw new ArgumentNullException("profiler");
            if (this.profiler != null) throw new InvalidOperationException("IProfiler already registered for this ConnectionMultiplexer");

            this.profiler = profiler;
            this.profiledCommands = new ConcurrentDictionary<object, ConcurrentAddOnlyBag<ProfileStorage>>();
        }

        /// <summary>
        /// Begins profiling for the given context.
        /// 
        /// If the same context object is returned by the registered IProfiler, the IProfiledCommands
        /// will be associated with each other.
        /// 
        /// Call FinishProfiling with the same context to get the assocated commands.
        /// </summary>
        public void BeginProfiling(object forContext)
        {
            if (profiler == null) throw new InvalidOperationException("Cannot begin profiling if no IProfiler has been registered with RegisterProfiler");
            if (forContext == null) throw new ArgumentNullException("forContext");
            if (!profiledCommands.TryAdd(forContext, new ConcurrentAddOnlyBag<ProfileStorage>()))
            {
                var exc = new InvalidOperationException("Attempted to begin profiling for the same context twice");
                exc.Data["forContext"] = forContext;
                throw exc;
            }
        }

        /// <summary>
        /// Stops profiling for the given context, returns all IProfiledCommands associated
        /// </summary>
        public IEnumerable<IProfiledCommand> FinishProfiling(object forContext)
        {
            if (profiler == null) throw new InvalidOperationException("Cannot begin profiling if no IProfiler has been registered with RegisterProfiler");
            if (forContext == null) throw new ArgumentNullException("forContext");

            ConcurrentAddOnlyBag<ProfileStorage> commands;
            if (!profiledCommands.TryRemove(forContext, out commands))
            {
                var exc = new InvalidOperationException("Attempted to finish profiling for a context which is no longer valid, or was never begun");
                exc.Data["forContext"] = forContext;
                throw exc;
            }

            return commands.Enumerate();
        }
    }
}
