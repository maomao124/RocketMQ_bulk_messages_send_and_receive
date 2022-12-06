package mao.producer;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Project name(项目名称)：RocketMQ_批量消息的发送与接收
 * Package(包名): mao.producer
 * Class(类名): ListSplitter
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/6
 * Time(创建时间)： 14:14
 * Version(版本): 1.0
 * Description(描述)： 将消息拆分，没次遍历得到的list集合的数据大小不大于4M
 */

public class ListSplitter implements Iterator<List<Message>>
{

    /**
     * 大小限制
     */
    private static final int SIZE_LIMIT = 1000 * 40;

    /**
     * 消息列表
     */
    private final List<Message> messages;

    /**
     * 当前索引位置
     */
    private int currIndex;

    /**
     * 构造方法
     *
     * @param messages 消息列表
     */
    public ListSplitter(List<Message> messages)
    {
        this.messages = messages;
    }

    @Override
    public boolean hasNext()
    {
        return currIndex < messages.size();
    }

    /**
     * 下一个
     *
     * @return {@link List}<{@link Message}>
     */
    @Override
    public List<Message> next()
    {
        //当前的起始索引
        int nextIndex = currIndex;
        //当前的总大小
        int totalSize = 0;
        //遍历
        for (; nextIndex < messages.size(); nextIndex++)
        {
            //取到消息对象
            Message message = messages.get(nextIndex);
            //计算当前消息对象的大小
            int tmpSize = message.getTopic().length() + message.getBody().length;
            //消息属性
            Map<String, String> messageProperties = message.getProperties();
            //遍历属性
            for (Map.Entry<String, String> entry : messageProperties.entrySet())
            {
                //计算属性的大小，添加进tmpSize
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            //日志-20字节
            tmpSize = tmpSize + 20;
            //判断当前大小(当前一个消息的大小)是否超过了限制的最大大小
            if (tmpSize > SIZE_LIMIT)
            {
                //超过了限制的最大大小
                if (nextIndex - currIndex == 0)
                {
                    //假如下一个子列表没有元素,则添加这个子列表然后退出循环,否则只是退出循环
                    nextIndex++;
                }
                break;
            }
            //当前遍历的总大小加上这次遍历的大小大于限制的大小
            if (tmpSize + totalSize > SIZE_LIMIT)
            {
                //直接退出循环
                break;
            }
            else
            {
                //当前遍历的总大小加上这次遍历的大小没有大于限制的大小
                //继续遍历
                totalSize += tmpSize;
            }
        }
        //生成列表并返回
        List<Message> subList = messages.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }
}
