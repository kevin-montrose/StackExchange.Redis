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
    sealed class ProfiledCommandCollection<T>
        where T : class
    {
        sealed class Node<NodeType>
            where NodeType : class
        {
            public NodeType Value;
            public Node<NodeType> Next;
        }

        volatile Node<T> Head;

        public ProfiledCommandCollection() { }

        public void Add(T command)
        {
            var newNode =
                new Node<T>
                {
                    Value = command
                };

            while(true)
            {
                var cur = Head;
                newNode.Next = cur;

                var got = Interlocked.CompareExchange(ref Head, newNode, cur);
                if (object.ReferenceEquals(got, cur)) break;
            }
        }

        public IEnumerable<T> Enumerate()
        {
            var cur = Head;
            while(cur != null)
            {
                yield return cur.Value;
                cur = cur.Next;
            }
        }
    }

    partial class ConnectionMultiplexer
    {
        private IProfiler profiler;
        private ConcurrentDictionary<object, ProfiledCommandCollection<IProfiledCommand>> profiledCommands;
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
            this.profiledCommands = new ConcurrentDictionary<object, ProfiledCommandCollection<IProfiledCommand>>();
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
            if (!profiledCommands.TryAdd(forContext, new ProfiledCommandCollection<IProfiledCommand>()))
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

            ProfiledCommandCollection<IProfiledCommand> commands;
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
