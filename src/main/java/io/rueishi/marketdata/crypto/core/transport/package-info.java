/**
 * Transport contracts for market-data connectivity.
 *
 * <p>Classes in this package define the shared transport surface used by
 * connector implementations, including connection lifecycle, inbound frame
 * delivery, and Netty WebSocket integration points. Connector and recovery code
 * own retry policy and observability mutation; transport classes only expose
 * events and channel state.</p>
 */
package io.rueishi.marketdata.crypto.core.transport;
