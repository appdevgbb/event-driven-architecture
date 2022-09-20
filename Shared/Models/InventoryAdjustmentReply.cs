namespace Shared.Models;

public enum InventoryAdjustmentStatus
{
    SUCCESS,
    INSUFFICIENT_INVENTORY
}
public class InventoryAdjustmentReply
{
    public int AdjustmentAmount { get; set; }
    public InventoryAdjustmentStatus InventoryAdjustmentStatus { get; set; }
}
