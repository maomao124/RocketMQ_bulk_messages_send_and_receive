package mao;

import mao.producer.BulkProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Project name(项目名称)：RocketMQ_批量消息的发送与接收
 * Package(包名): mao
 * Class(类名): Run
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/6
 * Time(创建时间)： 15:36
 * Version(版本): 1.0
 * Description(描述)： 无
 */

public class Run
{
    public static void main(String[] args)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException
    {
        while (true)
        {
            BulkProducer.main(null);
        }
    }
}
