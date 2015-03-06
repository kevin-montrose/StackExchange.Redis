using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    class ProfileStorage : IProfiledCommand
    {
        #region IProfiledCommand Impl
        public EndPoint EndPoint
        {
            get { return Server.EndPoint; }
        }

        public int Db
        {
            get { return Message.Db; }
        }

        public string Command
        {
            get { return Message.Command.ToString(); }
        }

        public DateTime CommandCreated
        {
            get { return MessageCreatedDateTime; }
        }

        public TimeSpan CreationToEnqueued
        {
            get { return TimeSpan.FromTicks(EnqueuedTimeStamp - MessageCreatedTimeStamp); }
        }

        public TimeSpan EnqueuedToSending
        {
            get { return TimeSpan.FromTicks(RequestSentTimeStamp - EnqueuedTimeStamp); }
        }

        public TimeSpan SentToResponse
        {
            get { return TimeSpan.FromTicks(ResponseReceivedTimeStamp - RequestSentTimeStamp); }
        }

        public TimeSpan ElapsedTime
        {
            get { return TimeSpan.FromTicks(ResponseReceivedTimeStamp - MessageCreatedTimeStamp); }
        }
        #endregion

        private Message Message;
        private ServerEndPoint Server;

        private DateTime MessageCreatedDateTime;
        private long MessageCreatedTimeStamp;
        private long EnqueuedTimeStamp;
        private long RequestSentTimeStamp;
        private long ResponseReceivedTimeStamp;

        private IProfilerEventSink EventSink;

        public ProfileStorage(IProfilerEventSink sink, ServerEndPoint server)
        {
            EventSink = sink;
            Server = server;
        }

        public void SetMessage(Message msg)
        {
            // This method should never be called twice
            if (Message != null) throw new InvalidOperationException();

            Message = msg;
            MessageCreatedDateTime = msg.createdDateTime;
            MessageCreatedTimeStamp = msg.createdTimestamp;
        }

        public void SetEnqueued()
        {
            // This method should never be called twice
            if (EnqueuedTimeStamp > 0) throw new InvalidOperationException();

            EnqueuedTimeStamp = Stopwatch.GetTimestamp();
        }

        public void SetRequestSent()
        {
            // This method should never be called twice
            if (RequestSentTimeStamp > 0) throw new InvalidOperationException();

            RequestSentTimeStamp = Stopwatch.GetTimestamp();
        }

        public void SetResponseReceived()
        {
            // this method can be called multiple times, depending on how the task completed (async vs not)
            //   so we actually have to guard against it.

            var now = Stopwatch.GetTimestamp();
            var oldVal = Interlocked.CompareExchange(ref ResponseReceivedTimeStamp, now, 0);

            // second call
            if (oldVal != 0) return;

            // only push to the EventSink on the first call
            EventSink.FinishProfiling(this);
        }
    }
}
