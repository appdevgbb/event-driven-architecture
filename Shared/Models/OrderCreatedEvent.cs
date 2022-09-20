namespace Shared.Models;
public class OrderCreatedEvent
{
    public Guid OrderId { get; set; }
    public int Quantity { get; set; }
    public int AccountNumber { get; set; }
}
