package com.lht.lhtmq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Leo
 * @date 2024/06/27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private long id;
    private String item;
    private double price;
}
