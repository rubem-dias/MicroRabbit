using MediatR;
using MicroRabbit.Domain.Core.Bus;
using MicroRabbit.Domain.Core.Commands;
using MicroRabbit.Domain.Core.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Text;

namespace MicroRabbit.Infra.Bus
{
    public sealed class RabbitMQBus : IEventBus
    {

        private readonly IMediator _mediator;
        private readonly Dictionary<string, List<Type>> _handlers;
        private readonly List<Type> _eventTypes;
        private readonly ConnectionFactory _factory;

        public RabbitMQBus(IMediator mediator)
        {
            _mediator = mediator;
            _handlers = new Dictionary<string, List<Type>>();
            _eventTypes = new List<Type>();
            _factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };
        }

        public Task SendCommand<T>(T command) where T : Command
        {
            return _mediator.Send(command);
        }

        public async Task PublishAsync<T>(T @event) where T : Event
        {
            try
            {
                await using var connection = await _factory.CreateConnectionAsync();
                await using var channel = await connection.CreateChannelAsync();

                var eventName = @event.GetType().Name;
                await channel.QueueDeclareAsync(eventName, false, false, false, null);

                var message = JsonConvert.SerializeObject(@event);
                var body = Encoding.UTF8.GetBytes(message).AsMemory();

                await channel.BasicPublishAsync(
                    exchange: "",
                    routingKey: eventName,
                    body: body
                );
            }
            catch (BrokerUnreachableException ex)
            {
                Console.WriteLine($"Erro ao conectar ao RabbitMQ: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao publicar mensagem: {ex.Message}");
            }
        }

        public void Subscribe<T, TH>()
            where T : Event
            where TH : IEventHandler<T>
        {
            var eventName = typeof(T).Name;
            var handlerType = typeof(TH);

            if (!_eventTypes.Contains(typeof(T)))
            {
                _eventTypes.Add(typeof(T));
            }

            if (!_handlers.ContainsKey(eventName))
            {
                _handlers.Add(eventName, new List<Type>());
            }

            if (_handlers[eventName].Any(s => s.GetType() == handlerType))
            {
                throw new ArgumentException($"Handler Type {handlerType.Name} already is registrered for " +
                    $"{eventName}", nameof(handlerType));
            }

            _handlers[eventName].Add(handlerType);

            Task.Run(async () => await StartBasicConsume<T>());
        }

        private async Task StartBasicConsume<T>() where T : Event
        {
            var connection = await _factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();

            var eventName = typeof(T).Name;

            await channel.QueueDeclareAsync(eventName, false, false, false, null);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += Consumer_Received;

            channel.BasicConsumeAsync(eventName, true, consumer);
        }

        private async Task Consumer_Received(object sender, BasicDeliverEventArgs @event)
        {
            var eventName = @event.RoutingKey;
            var message = Encoding.UTF8.GetString(@event.Body.ToArray());

            await ProcessEvent(eventName, message).ConfigureAwait(false);
        }

        private async Task ProcessEvent(string eventName, string message)
        {
            if (!_handlers.ContainsKey(eventName))
            {
                var subscriptions = _handlers[eventName];
                foreach (var subscription in subscriptions)
                {
                    var handler = Activator.CreateInstance(subscription);
                    if (handler == null) continue;
                    var eventType = _eventTypes.SingleOrDefault(t => t.Name == eventName);
                    var @event = JsonConvert.DeserializeObject(message, eventType);

                    var concreteType = typeof(IEventHandler<>).MakeGenericType(eventType);
                    await (Task)concreteType.GetMethod("Handle").Invoke(handler, new object[] { @event });
                }
            }
        }
    }
}
