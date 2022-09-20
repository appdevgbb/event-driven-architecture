using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Shared.Adminisration;
using Shared.Models;

namespace AccountService;

public class Worker : BackgroundService
{
    private const int NUMBER_OF_ACCOUNTS = 5;
    private const decimal STARTING_CREDIT_LIMIT = 5000M;
    private readonly ILogger<Worker> _logger;
    private readonly ServiceBusAdministrationClient _sbAdminClient;
    private readonly ServiceBusClient _sbClient;
    private readonly Random _random = new Random();
    private readonly Object _accountBalanceLock = new Object();
    private IDictionary<int, decimal> _accountBalances = new Dictionary<int, decimal>();
    private ConcurrentDictionary<string, ServiceBusSender> _replyToSenders = new ConcurrentDictionary<string, ServiceBusSender>();

    public Worker(ServiceBusAdministrationClient sbAdminClient, ServiceBusClient sbClient, ILogger<Worker> logger)
    {
        _sbAdminClient = sbAdminClient;
        _sbClient = sbClient;
        _logger = logger;

        for(int x = 1; x <= NUMBER_OF_ACCOUNTS; x++)
        {
            _accountBalances[x] = STARTING_CREDIT_LIMIT;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var messagingAdministration = new MessagingAdministration(sbAdminClient: _sbAdminClient);
        await messagingAdministration.EnsureMessagingEntities(cancellationToken: cancellationToken);

        var processor = _sbClient.CreateProcessor(queueName: MessagingAdministration.ACCOUNT_COMMANDS_QUEUE);
        processor.ProcessErrorAsync += ProcessErrorAsync;
        processor.ProcessMessageAsync += ProcessMessageAsync;
        
        await processor.StartProcessingAsync(cancellationToken: cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(10000, cancellationToken);
            foreach(var key in _accountBalances.Keys)
            {
                if(_accountBalances[key] < STARTING_CREDIT_LIMIT)
                {
                    var received = Math.Min((decimal)_random.NextDouble() * 400 + 100, STARTING_CREDIT_LIMIT - _accountBalances[key]);
                    _logger.LogInformation($"A payment for ${received:0.##} has been made on Account {key}!");
                    AdjustAccountAmount(key, (decimal)received, () => _accountBalances[key] < STARTING_CREDIT_LIMIT);
                }
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
            case nameof(AccountAdjustmentCommand):
                await ProcessAccountAdjustmentCommand(args);
                break;
            default:
                _logger.LogWarning($"Unknown MessageType received: {messageType}");
                break;
        }
    }

    private async Task ProcessAccountAdjustmentCommand(ProcessMessageEventArgs args)
    {
        var accountAdjustmentCommand = args.Message.Body.ToObjectFromJson<AccountAdjustmentCommand>();
        
        var accountAdjustmentStatus = AccountAdjustmentStatus.INVALID_ACCOUNT_NUMBER;
        if(_accountBalances.ContainsKey(accountAdjustmentCommand.AccountNumber))
        {
            _logger.LogInformation($"A command was received to adjust account {accountAdjustmentCommand.AccountNumber} by ${accountAdjustmentCommand.AdjustmentAmount}.");
            
            accountAdjustmentStatus = AdjustAccountAmount(accountAdjustmentCommand.AccountNumber, accountAdjustmentCommand.AdjustmentAmount, 
                                                        () => _accountBalances[accountAdjustmentCommand.AccountNumber] + accountAdjustmentCommand.AdjustmentAmount >= 0);
        }

        await SendAccountAdjustmentCommandReply(accountAdjustmentCommand.AdjustmentAmount, accountAdjustmentStatus, args.Message.ReplyTo, args.Message.ReplyToSessionId);
    }

    private Task SendAccountAdjustmentCommandReply(decimal adjustmentAmount, AccountAdjustmentStatus accountAdjustmentStatus, string replyTo, string replyToSessionId)
    {
        var accountCommandReplySender = _replyToSenders.GetOrAdd(replyTo, _sbClient.CreateSender(replyTo));
        
        return accountCommandReplySender.SendMessageAsync(new ServiceBusMessage
        {
            SessionId = replyToSessionId,
            ApplicationProperties = { { "MessageType", nameof(AccountAdjustmentReply) } },
            Body = BinaryData.FromObjectAsJson<AccountAdjustmentReply>(new AccountAdjustmentReply
            {
                AdjustmentAmount = adjustmentAmount,
                AccountAdjustmentStatus = accountAdjustmentStatus
            })
        });
    }

    private AccountAdjustmentStatus AdjustAccountAmount(int accountNumber, decimal amount, Func<bool> condition = null!)
    {
        lock(_accountBalanceLock)
        {
            var accountAdjustmentStatus = AccountAdjustmentStatus.INSUFFICIENT_CREDIT;
            if(condition == null || (condition != null && condition()))
            {
                _accountBalances[accountNumber] += amount;
                accountAdjustmentStatus = AccountAdjustmentStatus.SUCCESS;
            }
            _logger.LogInformation($"Current balance for account {accountNumber}: {_accountBalances[accountNumber]:0.##}");
            return accountAdjustmentStatus;
        }
    }
}

