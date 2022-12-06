package mao.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Project name(项目名称)：RocketMQ_批量消息的发送与接收
 * Package(包名): mao.producer
 * Class(类名): BulkProducer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/6
 * Time(创建时间)： 14:34
 * Version(版本): 1.0
 * Description(描述)： 批量消息-生产者
 */

public class BulkProducer
{
    /**
     * 得到int随机
     *
     * @param min 最小值
     * @param max 最大值
     * @return int
     */
    public static int getIntRandom(int min, int max)
    {
        if (min > max)
        {
            min = max;
        }
        return min + (int) (Math.random() * (max - min + 1));
    }

    public static String getBody()
    {
        StringBuilder stringBuilder = new StringBuilder(2000);
        for (int i = 0; i < getIntRandom(10, 500); i++)
        {
            stringBuilder.append(Integer.toBinaryString(getIntRandom(2000, 80000)));
        }
        return stringBuilder.toString();
    }


    public static void main(String[] args)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException
    {
        List<Message> messageList = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++)
        {
            Message message = new Message("test_topic", getBody().getBytes(StandardCharsets.UTF_8));
            messageList.add(message);
        }
        ListSplitter listSplitter = new ListSplitter(messageList);
        int i = 1;
        int position = 0;
        List<List<Message>> bulkMessageList = new ArrayList<>();
        while (listSplitter.hasNext())
        {
            System.out.println("-------------------------");
            System.out.println("第" + i + "次遍历");
            List<Message> messages = listSplitter.next();
            System.out.println("列表大小：" + messages.size());
            System.out.println("位置：" + position + "-" + (position + messages.size() - 1));
            System.out.println("-------------------------");
            i++;
            position = position + messages.size();
            bulkMessageList.add(messages);
        }


        System.out.println();
        System.out.println("开始发送批量消息");

        //生产者
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("mao_group");
        //设置nameserver地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //启动
        defaultMQProducer.start();
        //发送批量消息
        for (int i1 = 0; i1 < bulkMessageList.size(); i1++)
        {
            System.out.println("-------------------------");
            List<Message> messages = bulkMessageList.get(i1);
            System.out.println("正在发送第" + (i1 + 1) + "批消息");
            System.out.println("列表大小：" + messages.size());
            SendResult sendResult = defaultMQProducer.send(messages);
            //System.out.println("发送完成，发送结果：" + sendResult);
            System.out.println("-------------------------");
        }
        System.out.println("完成");
        defaultMQProducer.shutdown();
    }
}
