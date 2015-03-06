using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    partial class ConnectionMultiplexer
    {
        private IProfiler profiler;

        /// <summary>
        /// Sets an IProfiler instance for this ConnectionMultiplexer.
        /// </summary>
        public void RegisterProfiler(IProfiler profiler)
        {
            if (this.profiler != null) throw new InvalidOperationException("IProfiler already registered for this ConnectionMultiplexer");

            this.profiler = profiler;
        }
    }
}
