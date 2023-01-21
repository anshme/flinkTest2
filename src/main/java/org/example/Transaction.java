package org.example;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transaction{
    public String txn_id;
    public String amount;
    public String time;
    public Payer payer;
}