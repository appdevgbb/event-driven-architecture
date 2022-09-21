using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrderMaker.Models
{
    internal class OrderOutbox
    {

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "orderId")]
        public string OrderId { get; set; }

        [JsonProperty(PropertyName = "quantity")]
        public int Quantity { get; set; }

        [JsonProperty(PropertyName = "accountNumber")]
        public int AccountNumber { get; set; }

        [JsonProperty(PropertyName = "orderProcessed")]
        public bool OrderProcessed { get; set; }
    }
}
