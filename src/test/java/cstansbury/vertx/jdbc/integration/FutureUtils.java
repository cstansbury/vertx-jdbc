package cstansbury.vertx.jdbc.integration;

import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * 
 */
public class FutureUtils {

  // -------------------------------------------------------------------------
  // Static Methods
  // -------------------------------------------------------------------------

  /**
   * 
   * @param eventBus
   * @return
   */
  public static EventBusWrapper wrap(final EventBus eventBus) {
    return new EventBusWrapper(eventBus);
  }
  
  /**
   * 
   * @param consumer
   * @return
   */
  protected static <T> CompletableFuture<MessageWrapper<T>> getFuture(final Consumer<CompletableFuture<MessageWrapper<T>>> consumer) {
    final CompletableFuture<MessageWrapper<T>> future = new CompletableFuture<>();
    consumer.accept(future);
    return future;
  }
  
  /**
   * 
   * @param future
   * @param result
   */
  protected static <T> void handleResponse(final CompletableFuture<MessageWrapper<T>> future, final AsyncResult<Message<T>> result) {
    if (result.succeeded()) {
      future.complete(new MessageWrapper<T>(result.result()));
    } else {
      future.completeExceptionally(result.cause());
    }
  }

  // -------------------------------------------------------------------------
  // Inner Classes
  // -------------------------------------------------------------------------

  /**
   * 
   */
  public static class EventBusWrapper {
    
    private EventBus eventBus;
    
    /**
     * 
     * @param eventBus
     */
    public EventBusWrapper(final EventBus eventBus) {
      this.eventBus = eventBus;
    }
    
    /**
     * 
     * @param address
     * @param options
     * @param message
     * @return
     */
    public <T> CompletableFuture<MessageWrapper<T>> send(final String address, final DeliveryOptions options, final Message<T> message) {
      return getFuture(future -> {
        eventBus.send(address, message, options, (final AsyncResult<Message<T>> result) -> {
          handleResponse(future, result);
        });
      });
    }
    
    /**
     * 
     * @param address
     * @param options
     * @return
     */
    public <T> CompletableFuture<MessageWrapper<T>> send(final String address, final DeliveryOptions options) {
      return send(address, options, null);
    }
    
  }
  
  /**
   * 
   * @param <T>
   */
  public static class MessageWrapper<T> {
    
    private Message<T> message;
    
    /**
     * 
     * @param message
     */
    public MessageWrapper(final Message<T> message) {
      this.message = message;
    }
    
    /**
     * 
     * @param options
     * @param response
     * @return
     */
    public CompletableFuture<MessageWrapper<T>> sendReply(final DeliveryOptions options, final T response) {
      return getFuture(future -> {
        message.reply(response, options, (final AsyncResult<Message<T>> result) -> {
          handleResponse(future, result);
        });
      });
    }
    
    /**
     * 
     * @param options
     * @return
     */
    public CompletableFuture<MessageWrapper<T>> sendReply(final DeliveryOptions options) {
      return sendReply(options, null);
    }

  }
  
}
