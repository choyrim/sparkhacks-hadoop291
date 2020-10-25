package sparkhacks;

import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_PREFIX;
import static org.apache.hadoop.fs.s3a.S3AUtils.SSE_C_NO_KEY_ERROR;
import static org.apache.hadoop.fs.s3a.S3AUtils.SSE_S3_WITH_KEY_ERROR;

/**
 * An S3A Filesystem that works around
 * <a href="https://issues.apache.org/jira/browse/HADOOP-16794">HADOOP-16794</a>.
 * This rename implementation will respect s3 bucket default encryption properties.
 */
public class S3AFileSystem2ForHadoop291 extends S3AFileSystem {
    public static final Logger LOG = LoggerFactory.getLogger(S3AFileSystem2ForHadoop291.class);

    /**
     * Forward to the branch-3.3.0 implementation copy.
     *
     * The branch-3.3.0 implementation requires a source object metadata (<code>srcom</code>).
     * Although {@link #copyFile(String, String, long)} already obtains the <code>srcom</code>,
     * we do it again here to support a backward-compatible interface and avoid overriding
     * {@link #copyFile(String, String, long)}. Since this signature is used in 2.9.1 and
     * 3.2.1 implementations, this can substitute for both.
     */
    @Override
    protected void setOptionalCopyObjectRequestParameters(
            CopyObjectRequest copyObjectRequest) throws IOException {
        LOG.info("setOptionalCopyObjectRequestParameters[3.3.0]: {} -> {}",
                copyObjectRequest.getSourceKey(), copyObjectRequest.getDestinationKey());
        ObjectMetadata srcom = getObjectMetadata(copyObjectRequest.getSourceKey());
        setOptionalCopyObjectRequestParameters(srcom, copyObjectRequest);
    }

    /**
     * Additional initialization to support the branch-3.3.0 method implementations.
     * In particular, sets a {@link #encryptionSecrets} field.
     */
    @Override
    public void initialize(URI name, Configuration originalConf) throws IOException {
        super.initialize(name, originalConf);
        LOG.info("Additional initialization to support 3.3.0 method implementations");
        String bucket = getBucket();
        Configuration conf = getConf();
        // look for encryption data
        // DT Bindings may override this
        setEncryptionSecrets(new EncryptionSecrets(
                getEncryptionAlgorithm(bucket, conf),
                getServerSideEncryptionKey(bucket, conf)));
    }

    // --------------------------------------------------------------
    // copies of S3AFileSystem methods from branch-3.3.0
    // --------------------------------------------------------------

    /**
     * Propagate encryption parameters from source file if set else use the
     * current filesystem encryption settings.
     * @param srcom source object meta.
     * @param copyObjectRequest copy object request body.
     */
    private void setOptionalCopyObjectRequestParameters(
            ObjectMetadata srcom,
            CopyObjectRequest copyObjectRequest) {
        String sourceKMSId = srcom.getSSEAwsKmsKeyId();
        if (isNotEmpty(sourceKMSId)) {
            // source KMS ID is propagated
            LOG.debug("Propagating SSE-KMS settings from source {}",
                    sourceKMSId);
            copyObjectRequest.setSSEAwsKeyManagementParams(
                    new SSEAwsKeyManagementParams(sourceKMSId));
        }
        switch(getServerSideEncryptionAlgorithm()) {
            /**
             * Overriding with client encryption settings.
             */
            case SSE_C:
                generateSSECustomerKey().ifPresent(customerKey -> {
                    copyObjectRequest.setSourceSSECustomerKey(customerKey);
                    copyObjectRequest.setDestinationSSECustomerKey(customerKey);
                });
                break;
            case SSE_KMS:
                generateSSEAwsKeyParams().ifPresent(
                        copyObjectRequest::setSSEAwsKeyManagementParams);
                break;
            default:
        }
    }

    /**
     * Get the encryption algorithm of this endpoint.
     * @return the encryption algorithm.
     */
    public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
        return encryptionSecrets.getEncryptionMethod();
    }

    /**
     * Create the AWS SDK structure used to configure SSE,
     * if the encryption secrets contain the information/settings for this.
     * @return an optional set of KMS Key settings
     */
    private Optional<SSEAwsKeyManagementParams> generateSSEAwsKeyParams() {
        return EncryptionSecretOperations.createSSEAwsKeyManagementParams(
                encryptionSecrets);
    }

    /**
     * Create the SSE-C structure for the AWS SDK, if the encryption secrets
     * contain the information/settings for this.
     * This will contain a secret extracted from the bucket/configuration.
     * @return an optional customer key.
     */
    private Optional<SSECustomerKey> generateSSECustomerKey() {
        return EncryptionSecretOperations.createSSECustomerKey(
                encryptionSecrets);
    }

    /**
     * Set the encryption secrets for requests.
     * @param secrets secrets
     */
    protected void setEncryptionSecrets(final EncryptionSecrets secrets) {
        this.encryptionSecrets = secrets;
    }

    /**
     * This must never be null; until initialized it just declares that there
     * is no encryption.
     */
    private EncryptionSecrets encryptionSecrets = new EncryptionSecrets();

    // --------------------------------------------------------------
    // copies of S3AUtils methods from branch-3.3.0
    // --------------------------------------------------------------

    /**
     * Get any SSE key from a configuration/credential provider.
     * This operation handles the case where the option has been
     * set in the provider or configuration to the option
     * {@code OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY}.
     * IOExceptions raised during retrieval are swallowed.
     * @param bucket bucket to query for
     * @param conf configuration to examine
     * @return the encryption key or ""
     * @throws IllegalArgumentException bad arguments.
     */
    public static String getServerSideEncryptionKey(String bucket,
                                                    Configuration conf) {
        try {
            return lookupPassword(bucket, conf, SERVER_SIDE_ENCRYPTION_KEY);
        } catch (IOException e) {
            LOG.error("Cannot retrieve " + SERVER_SIDE_ENCRYPTION_KEY, e);
            return "";
        }
    }

    /**
     * Get the server-side encryption algorithm.
     * This includes validation of the configuration, checking the state of
     * the encryption key given the chosen algorithm.
     *
     * @param bucket bucket to query for
     * @param conf configuration to scan
     * @return the encryption mechanism (which will be {@code NONE} unless
     * one is set.
     * @throws IOException on any validation problem.
     */
    public static S3AEncryptionMethods getEncryptionAlgorithm(String bucket,
                                                              Configuration conf) throws IOException {
        S3AEncryptionMethods sse = S3AEncryptionMethods.getMethod(
                lookupPassword(bucket, conf,
                        SERVER_SIDE_ENCRYPTION_ALGORITHM));
        String sseKey = getServerSideEncryptionKey(bucket, conf);
        int sseKeyLen = StringUtils.isBlank(sseKey) ? 0 : sseKey.length();
        String diagnostics = passwordDiagnostics(sseKey, "key");
        switch (sse) {
            case SSE_C:
                LOG.debug("Using SSE-C with {}", diagnostics);
                if (sseKeyLen == 0) {
                    throw new IOException(SSE_C_NO_KEY_ERROR);
                }
                break;

            case SSE_S3:
                if (sseKeyLen != 0) {
                    throw new IOException(SSE_S3_WITH_KEY_ERROR
                            + " (" + diagnostics + ")");
                }
                break;

            case SSE_KMS:
                LOG.debug("Using SSE-KMS with {}",
                        diagnostics);
                break;

            case NONE:
            default:
                LOG.debug("Data is unencrypted");
                break;
        }
        return sse;
    }

    /**
     * Get a password from a configuration, including JCEKS files, handling both
     * the absolute key and bucket override.
     * @param bucket bucket or "" if none known
     * @param conf configuration
     * @param baseKey base key to look up, e.g "fs.s3a.secret.key"
     * @return a password or "".
     * @throws IOException on any IO problem
     * @throws IllegalArgumentException bad arguments
     */
    public static String lookupPassword(
            String bucket,
            Configuration conf,
            String baseKey)
            throws IOException {
        return lookupPassword(bucket, conf, baseKey, null, "");
    }

    /**
     * Provide a password diagnostics string.
     * This aims to help diagnostics without revealing significant password details
     * @param pass password
     * @param description description for text, e.g "key" or "password"
     * @return text for use in messages.
     */
    private static String passwordDiagnostics(String pass, String description) {
        if (pass == null) {
            return "null " + description;
        }
        int len = pass.length();
        switch (len) {
            case 0:
                return "empty " + description;
            case 1:
                return description + " of length 1";

            default:
                return description + " of length " + len + " ending with "
                        + pass.charAt(len - 1);
        }
    }

    /**
     * Get a password from a configuration, including JCEKS files, handling both
     * the absolute key and bucket override.
     * @param bucket bucket or "" if none known
     * @param conf configuration
     * @param baseKey base key to look up, e.g "fs.s3a.secret.key"
     * @param overrideVal override value: if non empty this is used instead of
     * querying the configuration.
     * @param defVal value to return if there is no password
     * @return a password or the value of defVal.
     * @throws IOException on any IO problem
     * @throws IllegalArgumentException bad arguments
     */
    public static String lookupPassword(
            String bucket,
            Configuration conf,
            String baseKey,
            String overrideVal,
            String defVal)
            throws IOException {
        String initialVal;
        Preconditions.checkArgument(baseKey.startsWith(FS_S3A_PREFIX),
                "%s does not start with $%s", baseKey, FS_S3A_PREFIX);
        // if there's a bucket, work with it
        if (StringUtils.isNotEmpty(bucket)) {
            String subkey = baseKey.substring(FS_S3A_PREFIX.length());
            String shortBucketKey = String.format(
                    BUCKET_PATTERN, bucket, subkey);
            String longBucketKey = String.format(
                    BUCKET_PATTERN, bucket, baseKey);

            // set from the long key unless overidden.
            initialVal = getPassword(conf, longBucketKey, overrideVal);
            // then override from the short one if it is set
            initialVal = getPassword(conf, shortBucketKey, initialVal);
        } else {
            // no bucket, make the initial value the override value
            initialVal = overrideVal;
        }
        return getPassword(conf, baseKey, initialVal, defVal);
    }

    /**
     * Get a password from a configuration, or, if a value is passed in,
     * pick that up instead.
     * @param conf configuration
     * @param key key to look up
     * @param val current value: if non empty this is used instead of
     * querying the configuration.
     * @return a password or "".
     * @throws IOException on any problem
     */
    private static String getPassword(Configuration conf, String key, String val)
            throws IOException {
        return getPassword(conf, key, val, "");
    }

    /**
     * Get a password from a configuration, or, if a value is passed in,
     * pick that up instead.
     * @param conf configuration
     * @param key key to look up
     * @param val current value: if non empty this is used instead of
     * querying the configuration.
     * @param defVal default value if nothing is set
     * @return a password or "".
     * @throws IOException on any problem
     */
    private static String getPassword(Configuration conf,
                                      String key,
                                      String val,
                                      String defVal) throws IOException {
        return isEmpty(val)
                ? lookupPassword(conf, key, defVal)
                : val;
    }

    /**
     * Get a password from a configuration/configured credential providers.
     * @param conf configuration
     * @param key key to look up
     * @param defVal value to return if there is no password
     * @return a password or the value in {@code defVal}
     * @throws IOException on any problem
     */
    static String lookupPassword(Configuration conf, String key, String defVal)
            throws IOException {
        try {
            final char[] pass = conf.getPassword(key);
            return pass != null ?
                    new String(pass).trim()
                    : defVal;
        } catch (IOException ioe) {
            throw new IOException("Cannot find password option " + key, ioe);
        }
    }

    private static final String BUCKET_PATTERN = FS_S3A_BUCKET_PREFIX + "%s.%s";

    // --------------------------------------------------------------
    // copies of required classes from branch-3.3.0
    // --------------------------------------------------------------

    /**
     * General IOException for Delegation Token issues.
     * Includes recommended error strings, which can be used in tests when
     * looking for specific errors.
     */
    public static class DelegationTokenIOException extends IOException {

        private static final long serialVersionUID = 599813827985340023L;

        /** Error: delegation token/token identifier class isn't the right one. */
        public static final String TOKEN_WRONG_CLASS
                = "Delegation token is wrong class";

        /**
         * The far end is expecting a different token kind than
         * that which the client created.
         */
        protected static final String TOKEN_MISMATCH = "Token mismatch";

        public DelegationTokenIOException(final String message) {
            super(message);
        }

        public DelegationTokenIOException(final String message,
                                          final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Encryption options in a form which can serialized or marshalled as a hadoop
     * Writeable.
     *
     * Maintainers: For security reasons, don't print any of this.
     *
     * Note this design marshalls/unmarshalls its serialVersionUID
     * in its writable, which is used to compare versions.
     *
     * <i>Important.</i>
     * If the wire format is ever changed incompatibly,
     * update the serial version UID to ensure that older clients get safely
     * rejected.
     *
     * <i>Important</i>
     * Do not import any AWS SDK classes, directly or indirectly.
     * This is to ensure that S3A Token identifiers can be unmarshalled even
     * without that SDK.
     */
    public static class EncryptionSecrets implements Writable, Serializable {

        public static final int MAX_SECRET_LENGTH = 2048;

        private static final long serialVersionUID = 1208329045511296375L;

        /**
         * Encryption algorithm to use: must match one in
         * {@link S3AEncryptionMethods}.
         */
        private String encryptionAlgorithm = "";

        /**
         * Encryption key: possibly sensitive information.
         */
        private String encryptionKey = "";

        /**
         * This field isn't serialized/marshalled; it is rebuilt from the
         * encryptionAlgorithm field.
         */
        private transient S3AEncryptionMethods encryptionMethod =
                S3AEncryptionMethods.NONE;

        /**
         * Empty constructor, for use in marshalling.
         */
        public EncryptionSecrets() {
        }

        /**
         * Create a pair of secrets.
         * @param encryptionAlgorithm algorithm enumeration.
         * @param encryptionKey key/key reference.
         * @throws IOException failure to initialize.
         */
        public EncryptionSecrets(final S3AEncryptionMethods encryptionAlgorithm,
                                 final String encryptionKey) throws IOException {
            this(encryptionAlgorithm.getMethod(), encryptionKey);
        }

        /**
         * Create a pair of secrets.
         * @param encryptionAlgorithm algorithm name
         * @param encryptionKey key/key reference.
         * @throws IOException failure to initialize.
         */
        public EncryptionSecrets(final String encryptionAlgorithm,
                                 final String encryptionKey) throws IOException {
            this.encryptionAlgorithm = encryptionAlgorithm;
            this.encryptionKey = encryptionKey;
            init();
        }

        /**
         * Write out the encryption secrets.
         * @param out {@code DataOutput} to serialize this object into.
         * @throws IOException IO failure
         */
        @Override
        public void write(final DataOutput out) throws IOException {
            new LongWritable(serialVersionUID).write(out);
            Text.writeString(out, encryptionAlgorithm);
            Text.writeString(out, encryptionKey);
        }

        /**
         * Read in from the writable stream.
         * After reading, call {@link #init()}.
         * @param in {@code DataInput} to deserialize this object from.
         * @throws IOException failure to read/validate data.
         */
        @Override
        public void readFields(final DataInput in) throws IOException {
            final LongWritable version = new LongWritable();
            version.readFields(in);
            if (version.get() != serialVersionUID) {
                throw new DelegationTokenIOException(
                        "Incompatible EncryptionSecrets version");
            }
            encryptionAlgorithm = Text.readString(in, MAX_SECRET_LENGTH);
            encryptionKey = Text.readString(in, MAX_SECRET_LENGTH);
            init();
        }

        /**
         * For java serialization: read and then call {@link #init()}.
         * @param in input
         * @throws IOException IO problem
         * @throws ClassNotFoundException problem loading inner class.
         */
        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            init();
        }

        /**
         * Init all state, including after any read.
         * @throws IOException error rebuilding state.
         */
        private void init() throws IOException {
            encryptionMethod = S3AEncryptionMethods.getMethod(
                    encryptionAlgorithm);
        }

        public String getEncryptionAlgorithm() {
            return encryptionAlgorithm;
        }

        public String getEncryptionKey() {
            return encryptionKey;
        }

        /**
         * Does this instance have encryption options?
         * That is: is the algorithm non-null.
         * @return true if there's an encryption algorithm.
         */
        public boolean hasEncryptionAlgorithm() {
            return org.apache.commons.lang3.StringUtils.isNotEmpty(encryptionAlgorithm);
        }

        /**
         * Does this instance have an encryption key?
         * @return true if there's an encryption key.
         */
        public boolean hasEncryptionKey() {
            return StringUtils.isNotEmpty(encryptionKey);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final EncryptionSecrets that = (EncryptionSecrets) o;
            return Objects.equals(encryptionAlgorithm, that.encryptionAlgorithm)
                    && Objects.equals(encryptionKey, that.encryptionKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(encryptionAlgorithm, encryptionKey);
        }

        /**
         * Get the encryption method.
         * @return the encryption method
         */
        public S3AEncryptionMethods getEncryptionMethod() {
            return encryptionMethod;
        }

        /**
         * String function returns the encryption mode but not any other
         * secrets.
         * @return a string safe for logging.
         */
        @Override
        public String toString() {
            return S3AEncryptionMethods.NONE.equals(encryptionMethod)
                    ? "(no encryption)"
                    : encryptionMethod.getMethod();
        }
    }

    /**
     * These support operations on {@link EncryptionSecrets} which use the AWS SDK
     * operations. Isolating them here ensures that that class is not required on
     * the classpath.
     */
    public static final class EncryptionSecretOperations {

        private EncryptionSecretOperations() {
        }

        /**
         * Create SSE-C client side key encryption options on demand.
         * @return an optional key to attach to a request.
         * @param secrets source of the encryption secrets.
         */
        public static Optional<SSECustomerKey> createSSECustomerKey(
                final EncryptionSecrets secrets) {
            if (secrets.hasEncryptionKey() &&
                    secrets.getEncryptionMethod() == S3AEncryptionMethods.SSE_C) {
                return Optional.of(new SSECustomerKey(secrets.getEncryptionKey()));
            } else {
                return Optional.empty();
            }
        }

        /**
         * Create SSE-KMS options for a request, iff the encryption is SSE-KMS.
         * @return an optional SSE-KMS param to attach to a request.
         * @param secrets source of the encryption secrets.
         */
        public static Optional<SSEAwsKeyManagementParams> createSSEAwsKeyManagementParams(
                final EncryptionSecrets secrets) {

            //Use specified key, otherwise default to default master aws/s3 key by AWS
            if (secrets.getEncryptionMethod() == S3AEncryptionMethods.SSE_KMS) {
                if (secrets.hasEncryptionKey()) {
                    return Optional.of(new SSEAwsKeyManagementParams(
                            secrets.getEncryptionKey()));
                } else {
                    return Optional.of(new SSEAwsKeyManagementParams());
                }
            } else {
                return Optional.empty();
            }
        }
    }
}
