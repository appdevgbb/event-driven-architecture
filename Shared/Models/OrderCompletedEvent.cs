namespace Shared.Models;
public class OrderCompletedEvent
{
    public OrderCompletedEvent()
    {
        
    }

    public OrderCompletedEvent(OrderCreatedEvent orderCreatedEvent)
    {
        OrderId = orderCreatedEvent.OrderId;
        Quantity = orderCreatedEvent.Quantity;
        AccountNumber = orderCreatedEvent.AccountNumber;
    }
    
    public Guid OrderId { get; set; }
    public int Quantity { get; set; }
    public int AccountNumber { get; set; }
}
