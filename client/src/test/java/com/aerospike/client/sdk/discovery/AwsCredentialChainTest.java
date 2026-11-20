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

import static com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProviderTest.KEY;
import static com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProviderTest.SECRET;
import static com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProviderTest.TOKEN;
import static com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProviderTest.assertNoSecret;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProvider.Credentials;
import com.aerospike.client.sdk.discovery.AwsSettings.AwsTweaks;
import com.aerospike.client.sdk.discovery.StubAwsServer.Reply;
import com.aerospike.client.sdk.discovery.StubAwsServer.Request;

/**
 * Each credential source of the AWS credential chain against {@link StubAwsServer}, chain order,
 * expiry and the hook.
 */
public class AwsCredentialChainTest {
	private static final SeedCandidateContext INIT = new SeedCandidateContext(null, new Host[0], 0);
	private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/seed-role";
	private static final String WEB_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVYQU1QTEUifQ.web-identity-token";

	private final List<String> messages = new CopyOnWriteArrayList<>();
	private PrintStream originalErr;
	private StubAwsServer stub;
	private TestAwsEnvironment env;
	private Path dir;

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

	@BeforeEach
	void setUp() throws Exception {
		stub = new StubAwsServer();
		env = new TestAwsEnvironment(stub.url());
		dir = Files.createTempDirectory("aws-chain");
		env.home = dir.toString();
		originalErr = System.err;
		System.setErr(new PrintStream(new LogCapture(), true, StandardCharsets.UTF_8));
	}

	@AfterEach
	void tearDown() throws Exception {
		System.setErr(originalErr);
		stub.close();

		try (Stream<Path> files = Files.walk(dir)) {
			files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
		}
	}

	/**
	 * The legacy builder(): region us-east-1, tag filter aerospike-cluster=prod-a and the test
	 * environment. {@code extra} adds to the AWS settings.
	 */
	private AwsEc2TagSeedCandidateProvider provider(Consumer<AwsTweaks> extra) {
		return new AwsEc2TagSeedCandidateProvider(SystemSettings.builder()
			.discovery(ops -> ops.aws(aws -> {
				aws.region("us-east-1")
					.filters(Map.of("tag:aerospike-cluster", List.of("prod-a")))
					.env(env);
				extra.accept(aws);
			}))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery()
			.getAws());
	}

	private AwsEc2TagSeedCandidateProvider provider() {
		return provider(aws -> {});
	}

	private Request lastEc2() {
		Request last = null;

		for (Request request : stub.requests()) {
			if (request.path.equals("/")) {
				last = request;
			}
		}
		return last;
	}

	private String refresh(AwsEc2TagSeedCandidateProvider provider) {
		provider.refreshSeedCandidates(INIT);
		return lastEc2().accessKey();
	}

	private void env(String key) {
		env.set("AWS_ACCESS_KEY_ID", key).set("AWS_SECRET_ACCESS_KEY", SECRET).set("AWS_SESSION_TOKEN", TOKEN);
	}

	private Path profileFile(String name) throws Exception {
		Path file = dir.resolve(name);
		Files.write(file, ("# shared credentials\n[default]\naws_access_key_id = AKIADEFAULT\n" +
			"aws_secret_access_key = " + SECRET + "\n\n[seed]\naws_access_key_id=AKIASEEDPROFILE\n" +
			"aws_secret_access_key=" + SECRET + "\naws_session_token=" + TOKEN + "\n\n[sso-only]\n" +
			"sso_session = corp\n").getBytes(StandardCharsets.UTF_8));
		return file;
	}

	private void webIdentity() throws Exception {
		Path token = dir.resolve("web-identity-token");
		Files.write(token, (WEB_TOKEN + "\n").getBytes(StandardCharsets.UTF_8));
		env.set("AWS_WEB_IDENTITY_TOKEN_FILE", token.toString()).set("AWS_ROLE_ARN", ROLE_ARN)
			.set("AWS_ROLE_SESSION_NAME", "seed-session");
		stub.sts(new Reply(200, StubAwsServer.assumeRoleWithWebIdentity("ASIASTS", SECRET, TOKEN, "2099-01-01T00:00:00Z")));
	}

	@Test
	void configKeys() {
		env("AKIAENV");
		assertEquals(KEY, refresh(provider(aws -> aws.credentials(KEY, SECRET, null))));
		assertNull(lastEc2().headers.get("x-amz-security-token"));
	}

	@Test
	void environment() {
		env("AKIAENV");
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("AKIAENV", refresh(provider));
		assertEquals("environment", provider.credentialSource());
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
		assertTrue(messages.contains("INFO AWS EC2 seed provider credentials from environment"), messages.toString());
		assertFalse(messages.toString().contains("AKIAENV"), messages.toString());
		assertNoSecret(messages.toString(), "");
	}

	@Test
	void profileFileWithAwsProfile() throws Exception {
		env.set("AWS_SHARED_CREDENTIALS_FILE", profileFile("credentials").toString()).set("AWS_PROFILE", "seed");
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("AKIASEEDPROFILE", refresh(provider));
		assertEquals("profile", provider.credentialSource());
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
	}

	@Test
	void profileFileDefaultInHome() throws Exception {
		Files.createDirectories(dir.resolve(".aws"));
		profileFile(".aws/credentials");
		assertEquals("AKIADEFAULT", refresh(provider()));
	}

	@Test
	void profileWithoutStaticKeysIsNotInChain() throws Exception {
		env.set("AWS_SHARED_CREDENTIALS_FILE", profileFile("credentials").toString()).set("AWS_PROFILE", "sso-only");
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("ASIAIMDS", refresh(provider));
		assertEquals("imds", provider.credentialSource());
	}

	@Test
	void webIdentityThroughStubSts() throws Exception {
		webIdentity();
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("ASIASTS", refresh(provider));
		assertEquals("web-identity", provider.credentialSource());

		Request sts = stub.requests().get(0);
		assertEquals("/sts", sts.path);
		assertEquals("AssumeRoleWithWebIdentity", sts.params.get("Action"));
		assertEquals("2011-06-15", sts.params.get("Version"));
		assertEquals(ROLE_ARN, sts.params.get("RoleArn"));
		assertEquals("seed-session", sts.params.get("RoleSessionName"));
		assertEquals(WEB_TOKEN, sts.params.get("WebIdentityToken"));
		assertNull(sts.headers.get("authorization"));
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
	}

	@Test
	void webIdentityStsErrorHasCodeAndNoToken() throws Exception {
		webIdentity();
		stub.sts(new Reply(400, "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error>" +
			"<Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>Token " + WEB_TOKEN + " is invalid</Message>" +
			"</Error><RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId></ErrorResponse>"));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider().refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage().contains("STS AssumeRoleWithWebIdentity failed: HTTP 400 " +
			"InvalidIdentityToken: Token <redacted> is invalid (request id c6104cbe-af31-11e0-8154-cbc7ccf896c7)"),
			ae.getMessage());
		assertEquals(0, stub.count("/"));
	}

	@Test
	void ecsWithAuthorizationToken() {
		env.set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1")
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN", StubAwsServer.ECS_AUTHORIZATION);
		stub.credentials(StubAwsServer.credentialsJson("ASIAECS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("ASIAECS", refresh(provider));
		assertEquals("ecs", provider.credentialSource());
		assertEquals("/ecs/v2/credentials/task-1", stub.requests().get(0).path);
	}

	@Test
	void ecsFullUriWithTokenFile() throws Exception {
		Path token = dir.resolve("ecs-token");
		Files.write(token, StubAwsServer.ECS_AUTHORIZATION.getBytes(StandardCharsets.UTF_8));
		env.set("AWS_CONTAINER_CREDENTIALS_FULL_URI", stub.url() + "/ecs/full")
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", token.toString())
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN", "wrong");
		stub.credentials(StubAwsServer.credentialsJson("ASIAECSFULL", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		assertEquals("ASIAECSFULL", refresh(provider()));
	}

	@Test
	void ecsWithoutAuthorizationFails() {
		env.set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1");
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider().refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage().contains("ECS credentials failed: HTTP 401"), ae.getMessage());
	}

	@Test
	void imdsV2TokenHandshake() {
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("ASIAIMDS", refresh(provider));
		assertEquals("imds", provider.credentialSource());

		List<Request> requests = stub.requests();
		assertEquals("PUT", requests.get(0).method);
		assertEquals("/latest/api/token", requests.get(0).path);
		assertEquals("21600", requests.get(0).headers.get("x-aws-ec2-metadata-token-ttl-seconds"));
		assertEquals("/latest/meta-data/iam/security-credentials/", requests.get(1).path);
		assertEquals("/latest/meta-data/iam/security-credentials/" + StubAwsServer.IMDS_ROLE, requests.get(2).path);
		assertEquals(StubAwsServer.IMDS_TOKEN, requests.get(2).headers.get("x-aws-ec2-metadata-token"));
	}

	@Test
	void noSourceFound() {
		env.set("AWS_EC2_METADATA_DISABLED", "true");
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider().refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage().contains("no AWS credentials found"), ae.getMessage());
		assertEquals(0, stub.requests().size());
	}

	@Test
	void chainOrderHigherSourceWins() throws Exception {
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		env.set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1")
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN", StubAwsServer.ECS_AUTHORIZATION);
		assertEquals("ASIAIMDS", refresh(provider()), "ecs over imds");

		webIdentity();
		assertEquals("ASIASTS", refresh(provider()), "web identity over ecs");

		env.set("AWS_SHARED_CREDENTIALS_FILE", profileFile("credentials").toString()).set("AWS_PROFILE", "seed");
		assertEquals("AKIASEEDPROFILE", refresh(provider()), "profile over web identity");

		env("AKIAENV");
		assertEquals("AKIAENV", refresh(provider()), "environment over profile");
		assertEquals(KEY, refresh(provider(aws -> aws.credentials(KEY, SECRET, null))), "config over environment");
		assertEquals("AKIAHOOK",
			refresh(provider(aws -> aws.credentialsHook(() -> new Credentials("AKIAHOOK", SECRET, null)))),
			"hook over config");
		assertEquals(1, stub.count("/sts"));
		assertEquals(0, stub.count("/latest/api/token"));
	}

	@Test
	void expiryRefreshesNearExpiryAndNotBefore() {
		String expiration = Instant.ofEpochMilli(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)).toString();
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS1", SECRET, TOKEN, expiration));
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("ASIAIMDS1", refresh(provider));
		assertEquals(1, stub.count("/latest/api/token"));
		assertEquals(1, stub.count("/latest/meta-data/iam/security-credentials/"));

		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS2", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		env.clockOffset = TimeUnit.MINUTES.toMillis(54);
		assertEquals("ASIAIMDS1", refresh(provider), "6 minutes before expiry");
		assertEquals(1, stub.count("/latest/api/token"));

		env.clockOffset = TimeUnit.MINUTES.toMillis(56);
		assertEquals("ASIAIMDS2", refresh(provider), "4 minutes before expiry");
		assertEquals(2, stub.count("/latest/api/token"));
		assertEquals(1, stub.count("/latest/meta-data/iam/security-credentials/"), "role name cached");
	}

	@Test
	void failedReloadKeepsUnexpiredCredentials() {
		String expiration = Instant.ofEpochMilli(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)).toString();
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS1", SECRET, TOKEN, expiration));
		AwsEc2TagSeedCandidateProvider provider = provider();
		assertEquals("ASIAIMDS1", refresh(provider));

		stub.credentials("{\"Code\":\"Failure\"}");
		env.clockOffset = TimeUnit.MINUTES.toMillis(58);
		assertEquals("ASIAIMDS1", refresh(provider));
		assertTrue(messages.stream().anyMatch(m -> m.startsWith("WARN AWS EC2 seed provider: " +
			"credentials refresh from imds failed, using cached credentials until they expire")), messages.toString());

		env.clockOffset = TimeUnit.MINUTES.toMillis(61);
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage().contains("IMDS credentials failed: Failure"), ae.getMessage());
	}

	@Test
	void hookReplacesChain() {
		env("AKIAENV");
		AtomicInteger calls = new AtomicInteger();
		AwsEc2TagSeedCandidateProvider provider = provider(aws -> aws.credentialsHook(() -> {
			calls.incrementAndGet();
			return new Credentials("ASIAHOOK", SECRET, TOKEN,
				System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(calls.get() == 1? 4 : 60));
		}));

		assertEquals("ASIAHOOK", refresh(provider));
		assertEquals("hook", provider.credentialSource());
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
		refresh(provider);
		refresh(provider);
		assertEquals(2, calls.get());
	}

	@Test
	void hookFailure() {
		AwsEc2TagSeedCandidateProvider provider = provider(aws -> aws.credentialsHook(() -> null));
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage().contains("credentials hook returned null"), ae.getMessage());

		AwsEc2TagSeedCandidateProvider throwing = provider(aws -> aws.credentialsHook(() -> {
			throw new IllegalStateException(SECRET);
		}));
		ae = assertThrows(AerospikeException.class, () -> throwing.refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage().contains("credentials hook failed: java.lang.IllegalStateException"), ae.getMessage());
		assertNoSecret(ae.getMessage(), "");
	}

	@Test
	void jsonParser() {
		assertEquals("{a=x\"y, n=12, b=true, s=é\n}", AwsCredentialChain.Json.object(
			"{\"a\":\"x\\\"y\", \"n\": 12, \"o\": {\"k\": [1, {\"z\": \"}\"}]}, \"b\":true, \"nil\": null, " +
			"\"s\": \"\\u00e9\\n\"}").toString());
		assertEquals("{}", AwsCredentialChain.Json.object(" { } ").toString());
		assertThrows(IllegalArgumentException.class, () -> AwsCredentialChain.Json.object("{\"a\" 1}"));
	}
}
