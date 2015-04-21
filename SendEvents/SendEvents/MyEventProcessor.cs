using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SendEvents
{
    public class MyEventProcessor : IEventProcessor
    {

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            return Task.FromResult(true);
        }

        public Task OpenAsync(PartitionContext context)
        {
            return Task.FromResult(true);
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var message in messages)
            {
                var eventType = message.Properties["event-type"];
                var bytes = message.GetBytes();
                if (eventType.ToString() == "utf8string")
                {
                    var body = System.Text.Encoding.UTF8.GetString(bytes);
                    Console.WriteLine("got:" + body);
                }
                else
                {
                    // Record that you don't know what to do with this event
                }
            }
            await context.CheckpointAsync();
        }
    }
}
