package org.example.modeldto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionDTO {
    public String txn_id;
    public String amount;
    public String time;
    public String name;
    public int age;
}
