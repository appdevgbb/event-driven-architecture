using System.Transactions;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Shared.Adminisration;
using Shared.Models;

namespace OrchestratorService;

public class Worker : BackgroundService
{
    private const decimal ITEM_PRICE = 50.00M;
    private readonly ILogger<Worker> _logger;
    private readonly ServiceBusAdministrationClient _sbAdminClient;
    private readonly ServiceBusClient _sbClient;
    private ServiceBusSender? _inventoryCommandSender;
    private ServiceBusSender? _accountCommandSender;
    private ServiceBusSender? _orderEventSender;

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
        
        _inventoryCommandSender = _sbClient.CreateSender(MessagingAdministration.INVENTORY_COMMANDS_QUEUE);
        _accountCommandSender = _sbClient.CreateSender(MessagingAdministration.ACCOUNT_COMMANDS_QUEUE);
        _orderEventSender = _sbClient.CreateSender(MessagingAdministration.ORDERS_TOPIC);

        var options = new ServiceBusSessionProcessorOptions
        {
            //MaxConcurrentSessions = 1,
            //MaxConcurrentCallsPerSession = 1,
            SessionIdleTimeout = TimeSpan.FromSeconds(5)
        };    

        var sessionProcessor = _sbClient.CreateSessionProcessor(queueName: MessagingAdministration.ORDER_ORCHESTRATION_QUEUE, options: options);
        sessionProcessor.SessionInitializingAsync += SessionInitializingAsync;
        sessionProcessor.SessionClosingAsync += SessionClosingAsync;
        sessionProcessor.ProcessErrorAsync += ProcessErrorAsync;
        sessionProcessor.ProcessMessageAsync += ProcessMessageAsync;
        
        await sessionProcessor.StartProcessingAsync(cancellationToken: cancellationToken);
    }

    private Task SessionInitializingAsync(ProcessSessionEventArgs args)
    {
        _logger.LogDebug($"Initializing session with ID: {args.SessionId}");
        return Task.CompletedTask;
    }

    private Task SessionClosingAsync(ProcessSessionEventArgs args)
    {
        _logger.LogDebug($"Closing session with ID: {args.SessionId}");
        return Task.CompletedTask;
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError($"Error: {args.Exception}");
        return Task.CompletedTask;
    }

    private async Task ProcessMessageAsync(ProcessSessionMessageEventArgs args)
    {
        using(var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            var messageType = args.Message.ApplicationProperties[MessagingAdministration.MESSAGE_TYPE_PROPERTY_NAME];

            switch(messageType)
            {
                case nameof(OrderCreatedEvent):
                    await ProcessOrderCreatedEventAsync(args);
                    break;
                case nameof(InventoryAdjustmentReply):
                    await ProcessInventoryAdjustmentReplyAsync(args);
                    break;
                case nameof(AccountAdjustmentReply):
                    await ProcessAccountAdjustmentReplyAsync(args);
                    break;
                default:
                    _logger.LogWarning($"Unknown MessageType received: {messageType}");
                    break;
            }

            transactionScope.Complete();
        }
        args.ReleaseSession();
    }

    private async Task ProcessOrderCreatedEventAsync(ProcessSessionMessageEventArgs args)
    {
        var orderCreatedEvent = args.Message.Body.ToObjectFromJson<OrderCreatedEvent>();

        var inventoryAdjustmentAmount = orderCreatedEvent.Quantity * -1;
        var accountAdjustmentAmount = orderCreatedEvent.Quantity * ITEM_PRICE * -1;

        var inventoryCommandTask = SendInventoryAdjustmentCommand(inventoryAdjustmentAmount, args.Message.SessionId);
        var accountCommandTask = SendAccountAdjustmentCommand(orderCreatedEvent.AccountNumber, accountAdjustmentAmount, args.Message.SessionId);
        await Task.WhenAll(inventoryCommandTask, accountCommandTask);

        var orderState = new OrderState(orderCreatedEvent)
        {
            CurrentOrderState = OrderState.OrderStateEnum.PROCESSING,
            InventoryAdjustmentAmount = inventoryAdjustmentAmount,
            AccountAdjustmentAmount = accountAdjustmentAmount
        };
        await args.SetSessionStateAsync(BinaryData.FromObjectAsJson(orderState));
    }

    private Task SendInventoryAdjustmentCommand(int adjustmentAmount, string replyToSessionId)
    {
        return _inventoryCommandSender!.SendMessageAsync(new ServiceBusMessage
        {
            ReplyTo = MessagingAdministration.ORDER_ORCHESTRATION_QUEUE,
            ReplyToSessionId = replyToSessionId,
            ApplicationProperties = { { MessagingAdministration.MESSAGE_TYPE_PROPERTY_NAME, nameof(InventoryAdjustmentCommand) } },
            Body = BinaryData.FromObjectAsJson<InventoryAdjustmentCommand>(new InventoryAdjustmentCommand
            {
                AdjustmentAmount = adjustmentAmount
            })
        });
    }

    private async Task ProcessInventoryAdjustmentReplyAsync(ProcessSessionMessageEventArgs args)
    {
        var inventoryAdjustmentReply = args.Message.Body.ToObjectFromJson<InventoryAdjustmentReply>();
        
        var sessionState = await args.GetSessionStateAsync();
        var orderState = sessionState.ToObjectFromJson<OrderState>();
        orderState.InventoryAdjustmentStatus = inventoryAdjustmentReply.InventoryAdjustmentStatus;
        await args.SetSessionStateAsync(BinaryData.FromObjectAsJson<OrderState>(orderState));

        await ProcessSagaStatusAsync(orderState: orderState, args: args);
    }

    private Task SendAccountAdjustmentCommand(int accountNumber, decimal adjustmentAmount, string replyToSessionId)
    {
        return _accountCommandSender!.SendMessageAsync(new ServiceBusMessage
        {
            ReplyTo = MessagingAdministration.ORDER_ORCHESTRATION_QUEUE,
            ReplyToSessionId = replyToSessionId,
            ApplicationProperties = { { MessagingAdministration.MESSAGE_TYPE_PROPERTY_NAME, nameof(AccountAdjustmentCommand) } },
            Body = BinaryData.FromObjectAsJson<AccountAdjustmentCommand>(new AccountAdjustmentCommand
            {
                AccountNumber = accountNumber,
                AdjustmentAmount = adjustmentAmount
            })
        });
    }

    private async Task ProcessAccountAdjustmentReplyAsync(ProcessSessionMessageEventArgs args)
    {
        var accountAdjustmentReply = args.Message.Body.ToObjectFromJson<AccountAdjustmentReply>();
        
        var sessionState = await args.GetSessionStateAsync();
        var orderState = sessionState.ToObjectFromJson<OrderState>();
        orderState.AccountAdjustmentStatus = accountAdjustmentReply.AccountAdjustmentStatus;
        await args.SetSessionStateAsync(BinaryData.FromObjectAsJson<OrderState>(orderState));

        await ProcessSagaStatusAsync(orderState: orderState, args: args);
    }

    private Task PublishOrderCompletedEvent(OrderCompletedEvent orderCompletedEvent)
    {
        return _orderEventSender!.SendMessageAsync(new ServiceBusMessage
        {
            ApplicationProperties = { { MessagingAdministration.MESSAGE_TYPE_PROPERTY_NAME, nameof(OrderCompletedEvent) } },
            Body = BinaryData.FromObjectAsJson<OrderCompletedEvent>(orderCompletedEvent)
        });
    }

    private Task PublishOrderRejectedEvent(OrderRejectedEvent orderRejectedEvent)
    {
        return _orderEventSender!.SendMessageAsync(new ServiceBusMessage
        {
            ApplicationProperties = { { MessagingAdministration.MESSAGE_TYPE_PROPERTY_NAME, nameof(OrderRejectedEvent) } },
            Body = BinaryData.FromObjectAsJson<OrderRejectedEvent>(orderRejectedEvent)
        });
    }

    private async Task ProcessSagaStatusAsync(OrderState orderState, ProcessSessionMessageEventArgs args)
    {
        if(orderState.CurrentOrderState == OrderState.OrderStateEnum.PROCESSING)
        {
            // Invalid Account Number
            if(orderState.AccountAdjustmentStatus == AccountAdjustmentStatus.INVALID_ACCOUNT_NUMBER)
            {
                LogOrderRejected(orderState, $"{Enum.GetName<AccountAdjustmentStatus>(orderState.AccountAdjustmentStatus.Value)}");
                // Move Order State to REJECTED (terminal)
                orderState.CurrentOrderState = OrderState.OrderStateEnum.REJECTED;
                // Publish order rejected event
                await PublishOrderRejectedEvent(new OrderRejectedEvent(orderState.OrderCreatedEvent) { OrderRejectedReason = OrderRejectedReason.INVALID_ACCOUNT_NUMBER });
            }
            else if(orderState.AccountAdjustmentStatus != null && orderState.InventoryAdjustmentStatus != null)
            {
                // Insufficient Inventory
                if(orderState.AccountAdjustmentStatus == AccountAdjustmentStatus.SUCCESS && orderState.InventoryAdjustmentStatus != InventoryAdjustmentStatus.SUCCESS)
                {
                    LogOrderRejected(orderState, $"{Enum.GetName<InventoryAdjustmentStatus>(orderState.InventoryAdjustmentStatus.Value)}");
                    // Move Order State to REJECTED (terminal)
                    orderState.CurrentOrderState = OrderState.OrderStateEnum.REJECTED;
                    // Send account compensating transaction
                    await SendCompensatingAccountTransaction(orderState, args);
                    // Publish order rejected event
                    await PublishOrderRejectedEvent(new OrderRejectedEvent(orderState.OrderCreatedEvent) { OrderRejectedReason = OrderRejectedReason.INSUFFICIENT_INVENTORY });
                }
                // Insufficient Credit
                else if (orderState.AccountAdjustmentStatus != AccountAdjustmentStatus.SUCCESS && orderState.InventoryAdjustmentStatus == InventoryAdjustmentStatus.SUCCESS)
                {
                    LogOrderRejected(orderState, $"{Enum.GetName<AccountAdjustmentStatus>(orderState.AccountAdjustmentStatus.Value)}");
                    // Move Order State to REJECTED (terminal)
                    orderState.CurrentOrderState = OrderState.OrderStateEnum.REJECTED;
                    // Send inventory compensating transaction
                    await SendCompensatingInventoryTransaction(orderState, args);
                    // Publish order rejected event
                    await PublishOrderRejectedEvent(new OrderRejectedEvent(orderState.OrderCreatedEvent) { OrderRejectedReason = OrderRejectedReason.INSUFFICIENT_CREDIT });
                }
                // Insufficient Credit AND Inventory
                else if (orderState.AccountAdjustmentStatus != AccountAdjustmentStatus.SUCCESS && orderState.InventoryAdjustmentStatus != InventoryAdjustmentStatus.SUCCESS)
                {
                    LogOrderRejected(orderState, $"{Enum.GetName<InventoryAdjustmentStatus>(orderState.InventoryAdjustmentStatus.Value)} | {Enum.GetName<AccountAdjustmentStatus>(orderState.AccountAdjustmentStatus.Value)}");
                    // Move Order State to REJECTED (terminal)
                    orderState.CurrentOrderState = OrderState.OrderStateEnum.REJECTED;
                    // No need to send compensating transaction as both failed
                    // Publish order rejected event
                    await PublishOrderRejectedEvent(new OrderRejectedEvent(orderState.OrderCreatedEvent) { OrderRejectedReason = OrderRejectedReason.INSUFFICIENT_CREDIT_INSUFFICIENT_INVENTORY });
                }
                // All good
                else
                {
                    LogOrderCompleted(orderState);
                    // Move Order State to COMPLETED (terminal)
                    orderState.CurrentOrderState = OrderState.OrderStateEnum.COMPLETED;
                    // Publish order completed event
                    await PublishOrderCompletedEvent(new OrderCompletedEvent(orderState.OrderCreatedEvent));
                }
            }
            // Update session state
            await args.SetSessionStateAsync(BinaryData.FromObjectAsJson<OrderState>(orderState));
        }
    }

    private Task SendCompensatingAccountTransaction(OrderState orderState, ProcessSessionMessageEventArgs args)
    {
        _logger.LogInformation($"Sending compensating account transaction for Order. [OrderId: {orderState.OrderCreatedEvent.OrderId}, AccountNumber: {orderState.OrderCreatedEvent.AccountNumber}, Amount: {orderState.AccountAdjustmentAmount * -1}]");
        return SendAccountAdjustmentCommand(orderState.OrderCreatedEvent.AccountNumber, orderState.AccountAdjustmentAmount * -1, args.Message.SessionId);
    }

    private Task SendCompensatingInventoryTransaction(OrderState orderState, ProcessSessionMessageEventArgs args)
    {
        _logger.LogInformation($"Sending compensating inventory transaction for Order. [OrderId: {orderState.OrderCreatedEvent.OrderId}, Quantity: {orderState.OrderCreatedEvent.Quantity}]");
        return SendInventoryAdjustmentCommand(orderState.InventoryAdjustmentAmount * -1, args.Message.SessionId);
    }

    private void LogOrderCompleted(OrderState orderState)
    {
        _logger.LogInformation($"Order Completed. [OrderId: {orderState.OrderCreatedEvent.OrderId}, AccountNumber: {orderState.OrderCreatedEvent.AccountNumber}, Quantity: {orderState.OrderCreatedEvent.Quantity}]");
    }

    private void LogOrderRejected(OrderState orderState, string reason)
    {
        _logger.LogInformation($"Order Rejected. Reason: {reason}. [OrderId: {orderState.OrderCreatedEvent.OrderId}, AccountNumber: {orderState.OrderCreatedEvent.AccountNumber}, Quantity: {orderState.OrderCreatedEvent.Quantity}]");
    }
}
