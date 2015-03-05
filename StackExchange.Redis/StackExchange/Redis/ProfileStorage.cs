using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    class ProfileStorage
    {
        public long MessageCreated { get; private set; }
        public long Enqueued { get; private set; }
        public long RequestSent { get; set; }
        public long ResponseReceived { get; set; }

        public void SetMessageCreated(long timestamp)
        {
            MessageCreated = timestamp;
        }

        public void SetEnqueued()
        {
            Enqueued = Stopwatch.GetTimestamp();
        }

        public void SetRequestSent()
        {
            RequestSent = Stopwatch.GetTimestamp();
        }

        public void SetResponseReceived()
        {
            ResponseReceived = Stopwatch.GetTimestamp();
        }

        public void Reset()
        {
            MessageCreated = Enqueued = RequestSent = ResponseReceived = -1;
        }
    }
}
