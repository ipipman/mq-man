package cn.ipman.mq.core;

public interface IMListener<T> {

    void onMessage(IMMessage<? extends T> message);
}
