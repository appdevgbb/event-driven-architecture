namespace Shared.Models;
public class AccountAdjustmentCommand
{
    public int AccountNumber { get; set; }
    public decimal AdjustmentAmount { get; set; }
}