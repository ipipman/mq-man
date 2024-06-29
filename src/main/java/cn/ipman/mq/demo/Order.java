package cn.ipman.mq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private long id;
    private String item;
    private double price;

}
