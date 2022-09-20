namespace Shared.Models;

public enum OrderRejectedReason
{
    UNKNOWN,
    INVALID_ACCOUNT_NUMBER,
    INSUFFICIENT_CREDIT,
    INSUFFICIENT_INVENTORY,
    INSUFFICIENT_CREDIT_INSUFFICIENT_INVENTORY
}

public class OrderRejectedEvent
{
    public OrderRejectedEvent()
    {
    }

    public OrderRejectedEvent(OrderCreatedEvent orderCreatedEvent)
    {
        OrderId = orderCreatedEvent.OrderId;
        Quantity = orderCreatedEvent.Quantity;
        AccountNumber = orderCreatedEvent.AccountNumber;
    }

    public Guid OrderId { get; set; }
    public OrderRejectedReason OrderRejectedReason { get; set; }
    public int Quantity { get; set; }
    public int AccountNumber { get; set; }
}
