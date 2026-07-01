package com.aerospike.client.sdk;

import com.aerospike.client.sdk.policy.Behavior;

/**
 * Factory interface for creating custom {@link Session} subtypes.
 *
 * <p>Implement this interface to define how a specific session subtype is constructed.
 * The type parameter {@code S} ensures compile-time safety: the return type of
 * {@link Cluster#createSession(Behavior, SessionExtension)} is inferred from the
 * extension's {@code S}.</p>
 *
 * <p>Example usage (from an extension module):</p>
 * <pre>{@code
 * public class MySessionExtension implements SessionExtension<MySession> {
 *     @Override
 *     public MySession create(Cluster cluster, Behavior behavior) {
 *         return new MySession(cluster, behavior);
 *     }
 * }
 *
 * MySession session = cluster.createSession(behavior, new MySessionExtension());
 * }</pre>
 *
 * @param <S> the concrete session type produced by this extension
 * @see Cluster#createSession(Behavior, SessionExtension)
 * @see Session
 */
public interface SessionExtension<S extends Session> {

    /**
     * Creates a new session of type {@code S} for the given cluster and behavior.
     *
     * <p>The implementation is responsible for constructing the session subtype,
     * typically by calling {@link Session}'s protected constructor via a subclass.</p>
     *
     * @param cluster  the cluster this session will operate on
     * @param behavior the behavior configuration for the session
     * @return a new session instance of type {@code S}
     */
    S create(Cluster cluster, Behavior behavior);
}
