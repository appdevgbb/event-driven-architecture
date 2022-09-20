using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Shared.Adminisration;
using Shared.Models;

namespace InventoryService;

public class Worker : BackgroundService
{
    private const int MAX_INVENTORY_COUNT = 1000;
    private readonly ILogger<Worker> _logger;
    private readonly ServiceBusAdministrationClient _sbAdminClient;
    private readonly ServiceBusClient _sbClient;
    private readonly Random _random = new Random();
    private readonly Object _inventoryCountLock = new Object();
    private ConcurrentDictionary<string, ServiceBusSender> _replyToSenders = new ConcurrentDictionary<string, ServiceBusSender>();
    private int _currentInventoryCount = MAX_INVENTORY_COUNT;

    public Worker(ServiceBusAdministrationClient sbAdminClient, ServiceBusClient sbClient, ILogger<Worker> logger)
    {
        _sbAdminClient = sbAdminClient;
        _sbClient = sbClient;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var messagingAdministration = new MessagingAdministration(sbAdminClient: _sbAdminClient);
        await messagingAdministration.EnsureMessagingEntities(cancellationToken: cancellationToken);

        var processor = _sbClient.CreateProcessor(queueName: MessagingAdministration.INVENTORY_COMMANDS_QUEUE);
        processor.ProcessErrorAsync += ProcessErrorAsync;
        processor.ProcessMessageAsync += ProcessMessageAsync;
        
        await processor.StartProcessingAsync(cancellationToken: cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(10000, cancellationToken);
            if(_currentInventoryCount < MAX_INVENTORY_COUNT)
            {
                var received = Math.Min(_random.Next(10, 101), MAX_INVENTORY_COUNT - _currentInventoryCount);
                _logger.LogInformation($"A shipment arrived! Incrementing inventory by {received}.");
                AdjustInventoryAmount(received);
            }
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError($"Error: {args.Exception}");
        return Task.CompletedTask;
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        var messageType = args.Message.ApplicationProperties[MessagingAdministration.MESSAGE_TYPE_PROPERTY_NAME];
        
        switch(messageType)
        {
            case nameof(InventoryAdjustmentCommand):
                await ProcessInventoryAdjustmentCommand(args);
                break;
            default:
                _logger.LogWarning($"Unknown MessageType received: {messageType}");
                break;
        }
    }

    private async Task ProcessInventoryAdjustmentCommand(ProcessMessageEventArgs args)
    {
        var inventoryAdjustmentCommand = args.Message.Body.ToObjectFromJson<InventoryAdjustmentCommand>();
        _logger.LogInformation($"A command was received to adjust inventory by {inventoryAdjustmentCommand.AdjustmentAmount}.");

        var inventoryAdjustmentStatus = AdjustInventoryAmount(inventoryAdjustmentCommand.AdjustmentAmount, () => _currentInventoryCount + inventoryAdjustmentCommand.AdjustmentAmount >= 0);
        
        await SendInventoryAdjustmentCommandReply(inventoryAdjustmentCommand.AdjustmentAmount, inventoryAdjustmentStatus, args.Message.ReplyTo, args.Message.ReplyToSessionId);
    }

    private Task SendInventoryAdjustmentCommandReply(int adjustmentAmount, InventoryAdjustmentStatus inventoryAdjustmentStatus, string replyTo, string replyToSessionId)
    {
        var inventoryCommandReplySender = _replyToSenders.GetOrAdd(replyTo, _sbClient.CreateSender(replyTo));
        
        return inventoryCommandReplySender.SendMessageAsync(new ServiceBusMessage
        {
            SessionId = replyToSessionId,
            ApplicationProperties = { { "MessageType", nameof(InventoryAdjustmentReply) } },
            Body = BinaryData.FromObjectAsJson<InventoryAdjustmentReply>(new InventoryAdjustmentReply
            {
                AdjustmentAmount = adjustmentAmount,
                InventoryAdjustmentStatus = inventoryAdjustmentStatus
            })
        });
    }

    private InventoryAdjustmentStatus AdjustInventoryAmount(int amount, Func<bool> condition = null!)
    {
        lock(_inventoryCountLock)
        {
            var inventoryAdjustmentStatus = InventoryAdjustmentStatus.INSUFFICIENT_INVENTORY;
            if(condition == null || (condition != null && condition()))
            {
                _currentInventoryCount += amount;
                inventoryAdjustmentStatus = InventoryAdjustmentStatus.SUCCESS;
            }
            _logger.LogInformation($"Current inventory: {_currentInventoryCount}");
            return inventoryAdjustmentStatus;
        }
    }
}
