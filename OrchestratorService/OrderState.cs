using Shared.Models;

namespace OrchestratorService;
public class OrderState
{
    public enum OrderStateEnum
    {
        PROCESSING,
        COMPLETED,
        REJECTED
    }

    public OrderState(OrderCreatedEvent orderCreatedEvent)
    {
        OrderCreatedEvent = orderCreatedEvent;
    }

    public OrderCreatedEvent OrderCreatedEvent { get; private set; }
    public OrderStateEnum CurrentOrderState { get; set; }
    public int InventoryAdjustmentAmount { get; set; }
    public InventoryAdjustmentStatus? InventoryAdjustmentStatus { get; set; }
    public decimal AccountAdjustmentAmount { get; set; }
    public AccountAdjustmentStatus? AccountAdjustmentStatus { get; set; }
}    

