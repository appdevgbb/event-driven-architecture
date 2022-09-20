namespace Shared.Models;

public enum AccountAdjustmentStatus
{
    SUCCESS,
    INVALID_ACCOUNT_NUMBER,
    INSUFFICIENT_CREDIT
}

public class AccountAdjustmentReply
{
    public decimal AdjustmentAmount { get; set; }
    public AccountAdjustmentStatus AccountAdjustmentStatus { get; set; }
}