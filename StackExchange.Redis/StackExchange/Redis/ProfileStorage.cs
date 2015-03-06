using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
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
            Message = msg;
        }

        public void SetMessageCreated(DateTime datetime, long timestamp)
        {
            MessageCreatedDateTime = datetime;
            MessageCreatedTimeStamp = timestamp;
        }

        public void SetEnqueued()
        {
            EnqueuedTimeStamp = Stopwatch.GetTimestamp();
        }

        public void SetRequestSent()
        {
            RequestSentTimeStamp = Stopwatch.GetTimestamp();
        }

        public void SetResponseReceived()
        {
            ResponseReceivedTimeStamp = Stopwatch.GetTimestamp();
            EventSink.FinishProfiling(this);
        }
    }
}
