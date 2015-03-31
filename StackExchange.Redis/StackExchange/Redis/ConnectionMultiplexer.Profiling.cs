using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    partial class ConnectionMultiplexer
    {
        private IProfiler profiler;

        // internal for test purposes
        internal ProfileContextTracker profiledCommands;

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
            this.profiledCommands = new ProfileContextTracker();
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


            if (!profiledCommands.TryCreate(forContext))
            {
                var exc = new InvalidOperationException("Attempted to begin profiling for the same context twice");
                exc.Data["forContext"] = forContext;
                throw exc;
            }
        }

        /// <summary>
        /// Stops profiling for the given context, returns all IProfiledCommands associated.
        /// 
        /// By default this may do a sweep of for dead profiling contexts, you can disable this by passing "allowCleanupSweep: false".
        /// </summary>
        public IEnumerable<IProfiledCommand> FinishProfiling(object forContext, bool allowCleanupSweep = true)
        {
            if (profiler == null) throw new InvalidOperationException("Cannot begin profiling if no IProfiler has been registered with RegisterProfiler");
            if (forContext == null) throw new ArgumentNullException("forContext");

            IEnumerable<IProfiledCommand> ret;
            if (!profiledCommands.TryRemove(forContext, out ret))
            {
                var exc = new InvalidOperationException("Attempted to finish profiling for a context which is no longer valid, or was never begun");
                exc.Data["forContext"] = forContext;
                throw exc;
            }

            if (allowCleanupSweep)
            {
                profiledCommands.TryCleanup();
            }

            return ret;
        }
    }
}
