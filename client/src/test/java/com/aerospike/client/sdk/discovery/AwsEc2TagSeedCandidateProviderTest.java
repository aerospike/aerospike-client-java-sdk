/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Loggers;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProvider.AddressField;
import com.aerospike.client.sdk.discovery.AwsSettings.AwsTweaks;
import com.aerospike.client.sdk.discovery.StubAwsServer.Reply;
import com.aerospike.client.sdk.discovery.StubAwsServer.Request;

class AwsEc2TagSeedCandidateProviderTest {
	static final String KEY = "AKIDEXAMPLE";
	static final String SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
	static final String TOKEN = "FwoGZXIvYXdzEXAMPLESESSIONTOKEN";
	private static final SeedCandidateContext INIT = new SeedCandidateContext(null, new Host[0], 0);
	private static final SeedCandidateContext TEND = new SeedCandidateContext(null, new Host[0], 7);
	private static final String TAG = "tag:aerospike-cluster";
	private static final int MARGIN = 50;
	private static final int DEBUG = 10;

	private final List<String> messages = new CopyOnWriteArrayList<>();
	private PrintStream originalErr;
	private Field logLevel;
	private int originalLevel;
	private StubAwsServer stub;
	private TestAwsEnvironment env;

	/**
	 * The legacy log callback: {@code LEVEL message} for each line slf4j-simple prints as
	 * {@code [thread] LEVEL logger - message} to the current {@code System.err}.
	 */
	private final class LogCapture extends OutputStream {
		private final ByteArrayOutputStream line = new ByteArrayOutputStream();

		@Override
		public synchronized void write(int b) {
			if (b == '\n') {
				String text = line.toString(StandardCharsets.UTF_8);
				line.reset();
				int start = text.indexOf("] ");
				int end = text.indexOf(" - ");

				if (start >= 0 && end > start) {
					String level = text.substring(start + 2, text.indexOf(' ', start + 2));
					messages.add(level + ' ' + text.substring(end + 3));
				}
			}
			else {
				line.write(b);
			}
		}
	}

	/**
	 * The legacy test set the log level to DEBUG. slf4j-simple fixes a logger's level when it is
	 * created, so the provider's logger level is set directly and restored afterward.
	 */
	@BeforeEach
	void setUp() throws Exception {
		stub = new StubAwsServer();
		env = new TestAwsEnvironment(stub.url())
			.set("AWS_ACCESS_KEY_ID", KEY)
			.set("AWS_SECRET_ACCESS_KEY", SECRET)
			.set("AWS_SESSION_TOKEN", TOKEN);

		Logger logger = LoggerFactory.getLogger(Loggers.TEND);
		logLevel = logger.getClass().getDeclaredField("currentLogLevel");
		logLevel.setAccessible(true);
		originalLevel = logLevel.getInt(logger);
		logLevel.setInt(logger, DEBUG);

		originalErr = System.err;
		System.setErr(new PrintStream(new LogCapture(), true, StandardCharsets.UTF_8));
	}

	@AfterEach
	void tearDown() throws Exception {
		System.setErr(originalErr);
		logLevel.setInt(LoggerFactory.getLogger(Loggers.TEND), originalLevel);
		stub.close();
	}

	/**
	 * AWS settings as the client builds them: {@code tweaks} merged with the defaults.
	 */
	private static AwsSettings settings(Consumer<AwsTweaks> tweaks) {
		return SystemSettings.builder()
			.discovery(ops -> ops.aws(tweaks))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery()
			.getAws();
	}

	private static Map<String,List<String>> filters(String... nameValues) {
		Map<String,List<String>> map = new LinkedHashMap<>();
		map.put(TAG, List.of("prod-a"));

		for (int i = 0; i < nameValues.length; i += 2) {
			map.put(nameValues[i], Arrays.asList(nameValues[i + 1].split(",")));
		}
		return map;
	}

	/**
	 * The legacy builder(): region us-east-1, tag filter aerospike-cluster=prod-a, the test
	 * environment and a 5 s timeout. {@code extra} adds to or overrides these.
	 */
	private Consumer<AwsTweaks> base(Consumer<AwsTweaks> extra) {
		return aws -> {
			aws.region("us-east-1").filters(filters()).env(env).timeout(Duration.ofMillis(5000));
			extra.accept(aws);
		};
	}

	private AwsEc2TagSeedCandidateProvider provider(Consumer<AwsTweaks> extra) {
		return new AwsEc2TagSeedCandidateProvider(settings(base(extra)));
	}

	private AwsEc2TagSeedCandidateProvider provider() {
		return provider(aws -> {});
	}

	private static String all() {
		return StubAwsServer.describe(null,
			StubAwsServer.instance("i-0b", "10.0.0.2", "198.51.100.2", "ip-10-0-0-2.ec2.internal",
				"ec2-198-51-100-2.compute-1.amazonaws.com"),
			StubAwsServer.instance("i-0a", "10.0.0.1", null, "ip-10-0-0-1.ec2.internal", ""),
			StubAwsServer.instance("i-0c", "10.0.0.3", "198.51.100.3", "ip-10-0-0-3.ec2.internal",
				"ec2-198-51-100-3.compute-1.amazonaws.com"),
			StubAwsServer.instance("i-0d", "10.0.0.1", null, "ip-10-0-0-1.ec2.internal", null));
	}

	private static List<String> names(List<Host> hosts) {
		List<String> list = new ArrayList<>();

		for (Host host : hosts) {
			list.add(host.name + ':' + host.port + (host.tlsName != null? '/' + host.tlsName : ""));
		}
		return list;
	}

	@Test
	void eachAddressFieldAndMissingField() {
		Map<AddressField,List<String>> expected = new LinkedHashMap<>();
		expected.put(AddressField.PRIVATE_IP, Arrays.asList("10.0.0.1:3000", "10.0.0.2:3000", "10.0.0.3:3000"));
		expected.put(AddressField.PUBLIC_IP, Arrays.asList("198.51.100.2:3000", "198.51.100.3:3000"));
		expected.put(AddressField.PRIVATE_DNS, Arrays.asList("ip-10-0-0-1.ec2.internal:3000",
			"ip-10-0-0-2.ec2.internal:3000", "ip-10-0-0-3.ec2.internal:3000"));
		expected.put(AddressField.PUBLIC_DNS, Arrays.asList("ec2-198-51-100-2.compute-1.amazonaws.com:3000",
			"ec2-198-51-100-3.compute-1.amazonaws.com:3000"));
		stub.ec2Default(new Reply(200, all()));

		for (Map.Entry<AddressField,List<String>> entry : expected.entrySet()) {
			messages.clear();
			List<Host> hosts = provider(aws -> aws.addressField(entry.getKey())).refreshSeedCandidates(TEND);
			assertEquals(entry.getValue(), names(hosts));

			boolean skips = entry.getKey() == AddressField.PUBLIC_IP || entry.getKey() == AddressField.PUBLIC_DNS;
			assertEquals(skips, messages.stream().anyMatch(m ->
				m.equals("DEBUG AWS EC2 seed provider skipped 2 instances without " + entry.getKey().element)),
				messages.toString());
		}
	}

	@Test
	void portAndTlsNameCarried() {
		stub.ec2Default(new Reply(200, all()));
		List<Host> hosts = provider(aws -> aws.port(4333).tlsName("aerospike-tls")).refreshSeedCandidates(TEND);

		assertEquals(Arrays.asList("10.0.0.1:4333/aerospike-tls", "10.0.0.2:4333/aerospike-tls",
			"10.0.0.3:4333/aerospike-tls"), names(hosts));
	}

	@Test
	void filtersSentAsConfiguredWithRunningAddedAndSigned() {
		AwsEc2TagSeedCandidateProvider provider = provider(aws -> aws.filters(filters("subnet-id", "subnet-1,subnet-2")));
		provider.refreshSeedCandidates(TEND);

		assertEquals(1, stub.requests().size());
		Request request = stub.requests().get(0);

		Map<String,String> expected = new LinkedHashMap<>();
		expected.put("Action", "DescribeInstances");
		expected.put("Version", "2016-11-15");
		expected.put("Filter.1.Name", "tag:aerospike-cluster");
		expected.put("Filter.1.Value.1", "prod-a");
		expected.put("Filter.2.Name", "subnet-id");
		expected.put("Filter.2.Value.1", "subnet-1");
		expected.put("Filter.2.Value.2", "subnet-2");
		expected.put("Filter.3.Name", "instance-state-name");
		expected.put("Filter.3.Value.1", "running");
		assertEquals(expected, request.params);
		assertEquals("POST", request.method);
		assertEquals(TOKEN, request.headers.get("x-amz-security-token"));

		String date = request.headers.get("x-amz-date");
		String authorization = request.headers.get("authorization");
		assertTrue(authorization.matches("AWS4-HMAC-SHA256 Credential=" + KEY + "/\\d{8}/us-east-1/ec2/" +
			"aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, Signature=[0-9a-f]{64}"),
			authorization);

		TreeMap<String,String> headers = new TreeMap<>();
		headers.put("content-type", request.headers.get("content-type"));
		headers.put("host", request.headers.get("host"));
		headers.put("x-amz-date", date);
		headers.put("x-amz-security-token", TOKEN);
		StringBuilder body = new StringBuilder();

		for (Map.Entry<String,String> entry : request.params.entrySet()) {
			body.append(body.length() > 0? "&" : "").append(AwsSigV4.encode(entry.getKey())).append('=')
				.append(AwsSigV4.encode(entry.getValue()));
		}
		assertEquals(AwsSigV4.authorization(KEY, SECRET, "POST", "/", "", headers,
			body.toString().getBytes(StandardCharsets.UTF_8), "us-east-1", "ec2", date), authorization);
	}

	@Test
	void paginationFollowsNextToken() {
		stub.ec2(new Reply(200, StubAwsServer.describe("page-2-token",
				StubAwsServer.instance("i-1", "10.0.0.1", null, null, null))),
			new Reply(200, StubAwsServer.describe("page-3-token+/=",
				StubAwsServer.instance("i-2", "10.0.0.2", null, null, null))),
			new Reply(200, StubAwsServer.describe(null,
				StubAwsServer.instance("i-3", "10.0.0.3", null, null, null))));

		List<Host> hosts = provider().refreshSeedCandidates(TEND);
		List<Request> requests = stub.requests();

		assertEquals(3, requests.size());
		assertNull(requests.get(0).params.get("NextToken"));
		assertEquals("page-2-token", requests.get(1).params.get("NextToken"));
		assertEquals("page-3-token+/=", requests.get(2).params.get("NextToken"));
		assertEquals("instance-state-name", requests.get(2).params.get("Filter.2.Name"));
		assertEquals(Arrays.asList("10.0.0.1:3000", "10.0.0.2:3000", "10.0.0.3:3000"), names(hosts));
		assertTrue(messages.contains("DEBUG AWS EC2 seed provider us-east-1 returned " + hosts +
			" from 3 instances in 3 pages"), messages.toString());
	}

	@Test
	void apiErrorThrowsWithCodeAndRequestIdAndNoSecret() {
		stub.ec2Default(new Reply(403, StubAwsServer.error("UnauthorizedOperation",
			"You are not authorized to perform this operation.", "ea966190-f9aa-478e-9ede-example")));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider().refreshSeedCandidates(TEND));
		String authorization = stub.requests().get(0).headers.get("authorization");
		String signature = authorization.substring(authorization.lastIndexOf('=') + 1);

		assertTrue(ae.getMessage().contains("AWS EC2 seed provider: DescribeInstances failed: HTTP 403 " +
			"UnauthorizedOperation: You are not authorized to perform this operation. " +
			"(request id ea966190-f9aa-478e-9ede-example)"), ae.getMessage());
		assertNoSecret(ae.getMessage(), signature);
		assertNoSecret(messages.toString(), signature);
	}

	@Test
	void errorEchoingSecretMaterialIsRedacted() {
		stub.ec2Default(new Reply(400, StubAwsServer.error("AuthFailure", "token " + TOKEN + " secret " + SECRET,
			"req-1")));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider().refreshSeedCandidates(TEND));
		assertTrue(ae.getMessage().contains("AuthFailure: token <redacted> secret <redacted> (request id req-1)"),
			ae.getMessage());
		assertNoSecret(ae.getMessage(), "");
	}

	@Test
	void xmlWithDoctypeRejected() {
		stub.ec2Default(new Reply(200, "<?xml version=\"1.0\"?><!DOCTYPE r [<!ENTITY x SYSTEM \"file:///etc/passwd\">]>" +
			"<DescribeInstancesResponse><reservationSet>&x;</reservationSet></DescribeInstancesResponse>"));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider().refreshSeedCandidates(TEND));
		assertTrue(ae.getMessage().contains("invalid DescribeInstances response"), ae.getMessage());
		assertTrue(ae.getMessage().contains("DOCTYPE"), ae.getMessage());
	}

	@Test
	void unansweredStubReturnsWithinTimeoutAndInitTimeout() throws Exception {
		try (ServerSocket silent = StubAwsServer.silent()) {
			AwsEc2TagSeedCandidateProvider provider = provider(aws -> aws
				.endpoint("http://127.0.0.1:" + silent.getLocalPort())
				.timeout(Duration.ofMillis(80))
				.initTimeout(Duration.ofMillis(600)));

			long elapsedInit = elapsed(() -> provider.refreshSeedCandidates(INIT), "timed out after 600ms");
			long elapsedTend = elapsed(() -> provider.refreshSeedCandidates(TEND), "timed out after 80ms");

			assertTrue(elapsedInit >= 600 && elapsedInit <= 600 + MARGIN, elapsedInit + "ms");
			assertTrue(elapsedTend >= 80 && elapsedTend <= 80 + MARGIN, elapsedTend + "ms");
		}
	}

	@Test
	void unansweredCredentialEndpointBoundedByTimeout() throws Exception {
		try (ServerSocket silent = StubAwsServer.silent()) {
			TestAwsEnvironment imds = new TestAwsEnvironment("http://127.0.0.1:" + silent.getLocalPort());
			AwsEc2TagSeedCandidateProvider provider = new AwsEc2TagSeedCandidateProvider(settings(aws -> aws
				.region("us-east-1")
				.filters(filters())
				.endpoint(stub.url())
				.timeout(Duration.ofMillis(80))
				.env(imds)));

			long elapsed = elapsed(() -> provider.refreshSeedCandidates(TEND), "IMDS token timed out");
			assertTrue(elapsed >= 80 && elapsed <= 80 + MARGIN, elapsed + "ms");
		}
	}

	private static long elapsed(Runnable call, String message) {
		long begin = System.nanoTime();
		AerospikeException ae = assertThrows(AerospikeException.class, call::run);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		assertTrue(ae.getMessage().contains(message), ae.getMessage());
		return elapsed;
	}

	@Test
	void noNewThread() throws Exception {
		stub.ec2Default(new Reply(200, all()));

		try (ServerSocket silent = StubAwsServer.silent()) {
			TestAwsEnvironment imds = new TestAwsEnvironment(stub.url());
			stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
			Set<Thread> before = threads();

			List<SeedCandidateProvider> providers = new ArrayList<>();
			providers.add(provider());
			providers.add(new AwsEc2TagSeedCandidateProvider(settings(aws -> aws
				.region("us-east-1")
				.filters(Map.of("tag:a", List.of("b")))
				.env(imds))));
			AwsEc2TagSeedCandidateProvider hung = provider(aws -> aws
				.endpoint("http://127.0.0.1:" + silent.getLocalPort())
				.timeout(Duration.ofMillis(20)));

			for (SeedCandidateProvider provider : providers) {
				provider.refreshSeedCandidates(INIT);

				for (int i = 1; i <= 5; i++) {
					assertEquals(3, provider.refreshSeedCandidates(new SeedCandidateContext(null, new Host[0], i)).size());
				}
			}

			for (int i = 0; i < 3; i++) {
				assertThrows(AerospikeException.class, () -> hung.refreshSeedCandidates(TEND));
			}

			Set<Thread> added = new HashSet<>(threads());
			added.removeAll(before);

			assertTrue(added.isEmpty(), threadNames(added).toString());
			assertEquals("imds", ((AwsEc2TagSeedCandidateProvider)providers.get(1)).credentialSource());
		}
	}

	private static Set<Thread> threads() {
		Set<Thread> set = new HashSet<>();

		for (Thread thread : Thread.getAllStackTraces().keySet()) {
			if (thread.isAlive()) {
				set.add(thread);
			}
		}
		return set;
	}

	private static Set<String> threadNames(Set<Thread> threads) {
		Set<String> names = new TreeSet<>();

		for (Thread thread : threads) {
			names.add(thread.getName());
		}
		return names;
	}

	/**
	 * The SDK reports the default region as '' (the legacy builder had none, so 'null'), and the
	 * timeouts as Durations. The null addressField case builds the settings without the defaults,
	 * which would otherwise replace the null.
	 */
	@Test
	void invalidConfigRejectedAtConstruction() {
		Map<String,Consumer<AwsTweaks>> cases = new LinkedHashMap<>();
		cases.put("Invalid AWS EC2 seed provider region: ''", aws -> aws.filters(filters()));
		cases.put("Invalid AWS EC2 seed provider region: 'US East'", base(aws -> aws.region("US East")));
		cases.put("Invalid AWS EC2 seed provider filters: at least one filter is required", aws -> aws.region("us-east-1"));
		cases.put("Invalid AWS EC2 seed provider filter name: ''", base(aws -> aws.filters(filters("", "x"))));
		cases.put("Invalid AWS EC2 seed provider filter 'subnet-id' values: empty",
			base(aws -> aws.filters(Map.of(TAG, List.of("prod-a"), "subnet-id", List.of()))));
		cases.put("Invalid AWS EC2 seed provider filter 'subnet-id' value: 'null'",
			base(aws -> aws.filters(Map.of(TAG, List.of("prod-a"), "subnet-id", Collections.singletonList(null)))));
		cases.put("Invalid AWS EC2 seed provider filter name: 'instance-state-name': the provider adds instance-state-name=running",
			base(aws -> aws.filters(filters("instance-state-name", "stopped"))));
		cases.put("Invalid AWS EC2 seed provider port: 0", base(aws -> aws.port(0)));
		cases.put("Invalid AWS EC2 seed provider port: 65536", base(aws -> aws.port(65536)));
		cases.put("Invalid AWS EC2 seed provider tlsName: empty", base(aws -> aws.tlsName("")));
		cases.put("Invalid AWS EC2 seed provider timeout: PT0S", base(aws -> aws.timeout(Duration.ZERO)));
		cases.put("Invalid AWS EC2 seed provider initTimeout: PT-0.001S", base(aws -> aws.initTimeout(Duration.ofMillis(-1))));
		cases.put("Invalid AWS EC2 seed provider endpoint: 'ftp://ec2.example.com': expected http or https URL with a " +
			"host and no query", base(aws -> aws.endpoint("ftp://ec2.example.com")));
		cases.put("Invalid AWS EC2 seed provider endpoint: 'http://ec2 example'", base(aws -> aws.endpoint("http://ec2 example")));
		cases.put("Invalid AWS EC2 seed provider credentials: credentials and credentialsHook cannot both be set",
			base(aws -> aws.credentials(KEY, SECRET, null).credentialsHook(() -> null)));

		for (Map.Entry<String,Consumer<AwsTweaks>> entry : cases.entrySet()) {
			AerospikeException ae = assertThrows(AerospikeException.class,
				() -> new AwsEc2TagSeedCandidateProvider(settings(entry.getValue())), entry.getKey());
			assertTrue(ae.getMessage().contains(entry.getKey()), ae.getMessage());
		}

		AwsSettings.Builder builder = AwsSettings.builder();
		new AwsSettings.AwsTweaksImpl(builder).region("us-east-1").filters(filters()).addressField(null);
		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AwsEc2TagSeedCandidateProvider(builder.build()));
		assertTrue(ae.getMessage().contains("Invalid AWS EC2 seed provider addressField: null"), ae.getMessage());

		ae = assertThrows(AerospikeException.class, () -> settings(base(aws -> aws.credentials(KEY, "", null))));
		assertTrue(ae.getMessage().contains("secret access key is empty"), ae.getMessage());
	}

	@Test
	void gettersAndDefaults() {
		AwsEc2TagSeedCandidateProvider provider = new AwsEc2TagSeedCandidateProvider(settings(aws -> aws
			.region("eu-west-1")
			.filters(filters())));

		assertEquals("eu-west-1", provider.getRegion());
		assertEquals(Collections.singletonMap(TAG, Collections.singletonList("prod-a")), provider.getFilters());
		assertEquals(AddressField.PRIVATE_IP, provider.getAddressField());
		assertEquals(3000, provider.getPort());
		assertNull(provider.getTlsName());
		assertEquals(2000, provider.getTimeout());
		assertEquals(10000, provider.getInitTimeout());
		assertNull(provider.getEndpoint());
		assertTrue(provider.needsPeriodicRefresh());
		assertFalse(provider.supportsDeadline());
		assertEquals("aws-tags eu-west-1 {tag:aerospike-cluster=[prod-a]}", provider.toString());
		assertThrows(UnsupportedOperationException.class, () -> provider.getFilters().put("x", null));
		assertFalse(new AwsEc2TagSeedCandidateProvider.Credentials(KEY, SECRET, TOKEN).toString().contains(SECRET));
	}

	static void assertNoSecret(String text, String signature) {
		assertFalse(text.contains(SECRET), text);
		assertFalse(text.contains(TOKEN), text);

		if (! signature.isEmpty()) {
			assertFalse(text.contains(signature), text);
		}
	}
}
