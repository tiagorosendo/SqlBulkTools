using System;
// ReSharper disable CheckNamespace
// ReSharper disable UnusedMember.Global

namespace SqlBulkTools.TestCommon.Model
{
    public class Transaction
    {
        public int TransactionId { get; set; }
        public int ProductId { get; set; }
        public DateTime TransactionDate { get; set; }
        public int Quantity { get; set; }
        public decimal ActualCost { get; set; }
    }
}