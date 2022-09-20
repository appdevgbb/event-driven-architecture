using Azure.Messaging.ServiceBus.Administration;
using Shared.Models;

namespace Shared.Adminisration;

public class MessagingAdministration
{
    public const string MESSAGE_TYPE_PROPERTY_NAME = "MessageType";
    public const string ORDERS_TOPIC = "orders";
    public const string ORDER_ORCHESTRATION_SUBSCRIPTION = "order-orchestration";
    public const string ONLY_ORDER_CREATED_EVENTS_RULE = "only-order-created-events";
    public const string ORDER_ORCHESTRATION_QUEUE = "order-orchestration";
    public const string INVENTORY_COMMANDS_QUEUE = "inventory-commands";
    public const string ACCOUNT_COMMANDS_QUEUE = "account-commands";
    private readonly ServiceBusAdministrationClient _sbAdminClient;

    public MessagingAdministration(ServiceBusAdministrationClient sbAdminClient)
    {
        _sbAdminClient = sbAdminClient;
    }

    public async Task EnsureMessagingEntities(CancellationToken cancellationToken)
    {
        // Create Order Orchestration Queue
        if(!(await _sbAdminClient.QueueExistsAsync(name: ORDER_ORCHESTRATION_QUEUE, cancellationToken: cancellationToken)).Value)
        {
            var createQueueOptions = new CreateQueueOptions(name: ORDER_ORCHESTRATION_QUEUE)
            {
                RequiresSession = true
            };

            await _sbAdminClient.CreateQueueAsync(options: createQueueOptions, cancellationToken: cancellationToken);
        }

        // Create Inventory Commands Queue
        if(!(await _sbAdminClient.QueueExistsAsync(name: INVENTORY_COMMANDS_QUEUE, cancellationToken: cancellationToken)).Value)
        {
            await _sbAdminClient.CreateQueueAsync(name: INVENTORY_COMMANDS_QUEUE, cancellationToken: cancellationToken);
        }

        // Create Account Commands Queue
        if(!(await _sbAdminClient.QueueExistsAsync(name: ACCOUNT_COMMANDS_QUEUE, cancellationToken: cancellationToken)).Value)
        {
            await _sbAdminClient.CreateQueueAsync(name: ACCOUNT_COMMANDS_QUEUE, cancellationToken: cancellationToken);
        }

        // Create Orders Topic
        if(!(await _sbAdminClient.TopicExistsAsync(name: ORDERS_TOPIC, cancellationToken: cancellationToken)).Value)
        {
            var createTopicOptions = new CreateTopicOptions(name: ORDERS_TOPIC) { RequiresDuplicateDetection = true };
            await _sbAdminClient.CreateTopicAsync(createTopicOptions, cancellationToken: cancellationToken);
        }
        
        // Create Order Orchestration Subscription
        if(!(await _sbAdminClient.SubscriptionExistsAsync(topicName: ORDERS_TOPIC, subscriptionName: ORDER_ORCHESTRATION_SUBSCRIPTION, cancellationToken: cancellationToken)).Value)
        {
            var createSubscriptionOptions = new CreateSubscriptionOptions(topicName: ORDERS_TOPIC, subscriptionName: ORDER_ORCHESTRATION_SUBSCRIPTION)
            {
                ForwardTo = ORDER_ORCHESTRATION_QUEUE
            };

            await _sbAdminClient.CreateSubscriptionAsync(options: createSubscriptionOptions, cancellationToken:cancellationToken);
        }

        // Create Only Order Created Events Filter
        if(!(await _sbAdminClient.RuleExistsAsync(topicName: ORDERS_TOPIC, subscriptionName: ORDER_ORCHESTRATION_SUBSCRIPTION, ruleName: ONLY_ORDER_CREATED_EVENTS_RULE, cancellationToken: cancellationToken)).Value)
        {
            var filter = new CorrelationRuleFilter();
            filter.ApplicationProperties[MESSAGE_TYPE_PROPERTY_NAME] = nameof(OrderCreatedEvent);

            var createRuleOptions = new CreateRuleOptions(name: ONLY_ORDER_CREATED_EVENTS_RULE, filter: filter);

            await _sbAdminClient.DeleteRuleAsync(topicName: ORDERS_TOPIC, subscriptionName: ORDER_ORCHESTRATION_SUBSCRIPTION, RuleProperties.DefaultRuleName, cancellationToken: cancellationToken);
            await _sbAdminClient.CreateRuleAsync(topicName: ORDERS_TOPIC, subscriptionName: ORDER_ORCHESTRATION_SUBSCRIPTION, options: createRuleOptions, cancellationToken: cancellationToken);
        }
    }
}