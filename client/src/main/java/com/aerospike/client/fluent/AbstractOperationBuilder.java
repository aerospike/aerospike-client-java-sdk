package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Abstract base class for operation builders that provides common functionality
 * for building database operations.
 * 
 * <p>This class contains shared functionality between key-specific operations
 * ({@link OperationBuilder}) and set-level background operations
 * ({@link BackgroundOperationBuilder}).</p>
 * 
 * <p>Subclasses must implement the {@code execute()} method with their specific
 * return type (e.g., RecordStream for key operations, ExecuteTask for background operations).</p>
 * 
 * @param <T> the concrete builder type (for fluent method chaining)
 */
public abstract class AbstractOperationBuilder<T extends AbstractOperationBuilder<T>> extends AbstractFilterableBuilder {
    protected final Session session;
    protected final List<Operation> ops = new ArrayList<>();
    protected final OpType opType;
    protected long expirationInSeconds = 0;  // Default, get value from server
    
    /**
     * TTL constant: Record never expires (TTL = -1)
     */
    public static final int TTL_NEVER_EXPIRE = -1;
    
    /**
     * TTL constant: Do not change the current TTL of the record (TTL = -2)
     */
    public static final int TTL_NO_CHANGE = -2;
    
    /**
     * TTL constant: Use the server's default TTL for the namespace (TTL = 0)
     */
    public static final int TTL_SERVER_DEFAULT = 0;
    
    protected AbstractOperationBuilder(Session session, OpType opType) {
        this.session = session;
        this.opType = opType;
    }
    
    /**
     * Returns a bin builder for operating on a specific bin.
     * 
     * @param binName the name of the bin
     * @return BinBuilder for constructing bin operations
     */
    public BinBuilder<T> bin(String binName) {
        return new BinBuilder<>(self(), binName);
    }
    
    /**
     * Returns this builder cast to the concrete type.
     * Used for fluent method chaining.
     */
    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }
    
    /**
     * Set a bin value using a Bin object.
     * Protected method used by BinBuilder.
     */
    protected T setTo(Bin bin) {
        this.ops.add(Operation.put(bin));
        return self();
    }
    
    /**
     * Read a bin value.
     * Protected method used by BinBuilder.
     */
    protected T get(String binName) {
        this.ops.add(Operation.get(binName));
        return self();
    }
    
    /**
     * Append to a bin value.
     * Protected method used by BinBuilder.
     */
    protected T append(Bin bin) {
        this.ops.add(Operation.append(bin));
        return self();
    }
    
    /**
     * Prepend to a bin value.
     * Protected method used by BinBuilder.
     */
    protected T prepend(Bin bin) {
        this.ops.add(Operation.prepend(bin));
        return self();
    }
    
    /**
     * Add to a bin value (numeric addition).
     * Protected method used by BinBuilder.
     */
    protected T add(Bin bin) {
        this.ops.add(Operation.add(bin));
        return self();
    }
    
    /**
     * Add a custom operation.
     * Protected method used by BinBuilder.
     */
    protected T addOp(Operation op) {
        this.ops.add(op);
        return self();
    }
    
    /**
     * Set the expiration for the record relative to the current time.
     * 
     * @param duration The duration after which the record should expire
     * @return This builder for method chaining
     */
    public T expireRecordAfter(Duration duration) {
        this.expirationInSeconds = duration.toSeconds();
        return self();
    }
    
    /**
     * Set the expiration for the record relative to the current time.
     * 
     * @param expirationInSeconds The number of seconds after which the record should expire
     * @return This builder for method chaining
     */
    public T expireRecordAfterSeconds(int expirationInSeconds) {
        this.expirationInSeconds = expirationInSeconds;
        return self();
    }
    
    /**
     * Validate and calculate expiration from a Date object.
     */
    protected long getExpirationInSecondsAndCheckValue(Date date) {
        long expirationInSeconds = (date.getTime() - new Date().getTime()) / 1000L;
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }
    
    /**
     * Set the expiration for the record to an absolute date/time.
     * 
     * @param date The date at which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if the date is in the past
     */
    public T expireRecordAt(Date date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return self();
    }
    
    /**
     * Validate and calculate expiration from a LocalDateTime object.
     */
    protected long getExpirationInSecondsAndCheckValue(LocalDateTime date) {
        LocalDateTime now = LocalDateTime.now();
        long expirationInSeconds = ChronoUnit.SECONDS.between(now, date);
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }
    
    /**
     * Set the expiration for the record to an absolute date/time.
     * 
     * @param date The LocalDateTime at which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if the date is in the past
     */
    public T expireRecordAt(LocalDateTime date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return self();
    }
    
    /**
     * Do not change the expiration of the record (TTL = -2).
     * 
     * @return This builder for method chaining
     */
    public T withNoChangeInExpiration() {
        this.expirationInSeconds = TTL_NO_CHANGE;
        return self();
    }
    
    /**
     * Set the record to never expire (TTL = -1).
     * 
     * @return This builder for method chaining
     */
    public T neverExpire() {
        this.expirationInSeconds = TTL_NEVER_EXPIRE;
        return self();
    }
    
    /**
     * Use the server's default expiration for the record (TTL = 0).
     * 
     * @return This builder for method chaining
     */
    public T expiryFromServerDefault() {
        this.expirationInSeconds = TTL_SERVER_DEFAULT;
        return self();
    }
    
    /**
     * Convert expiration seconds to int, handling overflow.
     */
    protected int getExpirationAsInt(long expirationInSeconds) {
        if (expirationInSeconds > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) expirationInSeconds;
        }
    }
    
    /**
     * Get expiration as int for current operation.
     */
    protected int getExpirationAsInt() {
        return getExpirationAsInt(expirationInSeconds);
    }
    
    /**
     * Get the session associated with this builder.
     */
    protected Session getSession() {
        return this.session;
    }
}

