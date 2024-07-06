package cn.ipman.mq.client;


import cn.ipman.mq.model.Message;

public interface IMListener<T> {

    /**
     * 当收到消息时调用此方法。
     * <p>
     * 该方法是监听器的核心方法，用于处理接收到的消息。当消息队列中有新消息到达时，将会调用监听器实例的此方法。
     * 实现此方法的监听器需要根据接收到的消息内容执行相应的业务逻辑。
     *
     * @param message 接收到的消息对象。消息对象封装了消息的详细信息，如消息内容、消息类型等，监听器通过该参数获取并处理消息。
     *                使用泛型<? extends T>来确保消息类型与监听器处理的类型相匹配或其子类型，提高了代码的灵活性。
     */
    void onMessage(Message<? extends T> message);
}
