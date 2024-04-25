import java.nio.charset.StandardCharsets
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.RemoteResourceInfo
import net.schmizz.sshj.sftp.SFTPClient
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.FilePermission
import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.Validator
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processors.standard.util.FileInfo
import org.apache.nifi.processors.standard.util.FileTransfer
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.expression.AttributeExpression
import org.apache.nifi.expression.ExpressionLanguageScope

import java.text.DateFormat
import java.text.SimpleDateFormat

/*
 *    Implements a Scripted Processor
 *    - http://funnifi.blogspot.com.br/2016/02/invokescriptedprocessor-hello-world.html
 *    - https://static.javadoc.io/org.apache.nifi/nifi-api/1.4.0/org/apache/nifi/processor/Processor.html
 */
@EventDriven
@CapabilityDescription("Received Attribute from previous flowFile to pass to SFTP Listing")
class ListSFTPCustomV2Processor implements Processor {
    /*
    * Define Relationships (https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html#documenting-relationships)
    */
    final static String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime"
    final static String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime"
    final static String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime"
    final static String FILE_SIZE_ATTRIBUTE = "file.size"
    final static String FILE_OWNER_ATTRIBUTE = "file.owner"
    final static String FILE_GROUP_ATTRIBUTE = "file.group"
    final static String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions"
    final static String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ"

    final static Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description('FlowFiles that were successfully processed and had any data enriched are routed here')
        .build()

    final static Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description('FlowFiles that were not successfully processed are routed here')
        .build()

    final static Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description('The input flowfile gets sent to this relationship when the query succeeds.')
        .build()

    Set<Relationship> getRelationships() { [REL_FAILURE, REL_SUCCESS, REL_ORIGINAL] as Set }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() { null }

    /*
     * Processor initialization
     */
    def log
    void initialize(ProcessorInitializationContext context) { log = context.logger}
    
    /*
        * Processor execution
        * https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html#performing-the-work
        */
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        def session = sessionFactory.createSession()
        FlowFile flowFile = session.get()
        if(!flowFile) return
        
        // Get properties
        String hostname = flowFile.getAttribute('host_name')
        int port = 22
        String username = flowFile.getAttribute('username')
        String password = flowFile.getAttribute('password')
        String remotePath = "/home/nssapp/ftptest/radio/"
        // Initialize SSH client
        SSHClient sshClient = new SSHClient()
        sshClient.addHostKeyVerifier(new PromiscuousVerifier())
        sshClient.connect(hostname, port)
        sshClient.authPassword(username, password)
        // Initialize SFTP client
        SFTPClient sftpClient = sshClient.newSFTPClient()

        final List<FileInfo> listing = new ArrayList<>(1000)

        try {
            // List files in the specified remote directory
            List<RemoteResourceInfo> files = sftpClient.ls(remotePath)
            for (RemoteResourceInfo file : files) {
                listing.add(newFileInfo(file, remotePath))
            }

            createFlowFilesForEntities(flowFile, session, listing)
        } catch (final Throwable t) {
            log.error('{} failed to process due to {}', [this, t] as Object[])
        } finally {
            sftpClient.close()
            sshClient.disconnect()

            session.transfer(flowFile, REL_ORIGINAL)
            session.commit()
        }
    }

    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) { null }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) { }

    @Override
    String getIdentifier() { null }

    protected String getProtocolName() {
        return "sftp"
    }

    private FileInfo newFileInfo(final RemoteResourceInfo entry, String path) {
        if (entry == null) {
            return null
        }
        final File newFullPath = new File(path, entry.getName())
        final String newFullForwardPath = newFullPath.getPath().replace("\\", "/")

        final StringBuilder permsBuilder = new StringBuilder()
        final Set<FilePermission> permissions = entry.getAttributes().getPermissions()

        appendPermission(permsBuilder, permissions, FilePermission.USR_R, "r")
        appendPermission(permsBuilder, permissions, FilePermission.USR_W, "w")
        appendPermission(permsBuilder, permissions, FilePermission.USR_X, "x")

        appendPermission(permsBuilder, permissions, FilePermission.GRP_R, "r")
        appendPermission(permsBuilder, permissions, FilePermission.GRP_W, "w")
        appendPermission(permsBuilder, permissions, FilePermission.GRP_X, "x")

        appendPermission(permsBuilder, permissions, FilePermission.OTH_R, "r")
        appendPermission(permsBuilder, permissions, FilePermission.OTH_W, "w")
        appendPermission(permsBuilder, permissions, FilePermission.OTH_X, "x")

        final FileInfo.Builder builder = new FileInfo.Builder()
            .filename(entry.getName())
            .fullPathFileName(newFullForwardPath)
            .directory(entry.isDirectory())
            .size(entry.getAttributes().getSize())
            .lastModifiedTime(entry.getAttributes().getMtime() * 1000L)
            .permissions(permsBuilder.toString())
            .owner(Integer.toString(entry.getAttributes().getUID()))
            .group(Integer.toString(entry.getAttributes().getGID()))
        return builder.build()
    }

    private void appendPermission(final StringBuilder builder, final Set<FilePermission> permissions, final FilePermission filePermission, final String permString) {
        if (permissions.contains(filePermission)) {
            builder.append(permString)
        } else {
            builder.append("-")
        }
    }

    private void createFlowFilesForEntities(final FlowFile flowFile, final ProcessSession session, final List<FileInfo> entities) {
        for (final FileInfo entity : entities) {
            // Create the FlowFile for this path.
            final Map<String, String> attributes = createAttributes(entity, flowFile)
            FlowFile newFlowFlie = session.create()
            newFlowFlie = session.putAllAttributes(newFlowFlie, attributes)
            session.transfer(newFlowFlie, REL_SUCCESS)
        }
    }

    protected Map<String, String> createAttributes(final FileInfo fileInfo, final FlowFile flowFile) {
        final Map<String, String> attributes = new HashMap<>()
        final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US)
        attributes.put(getProtocolName() + ".remote.host", flowFile.getAttribute('host_name'))
        attributes.put(getProtocolName() + ".remote.port", "22")
        attributes.put(getProtocolName() + ".listing.user", flowFile.getAttribute('username'))
        attributes.put(getProtocolName() + ".listing.pass", flowFile.getAttribute('password'))
        attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(fileInfo.getLastModifiedTime())))
        attributes.put(FILE_PERMISSIONS_ATTRIBUTE, fileInfo.getPermissions())
        attributes.put(FILE_OWNER_ATTRIBUTE, fileInfo.getOwner())
        attributes.put(FILE_GROUP_ATTRIBUTE, fileInfo.getGroup())
        attributes.put(FILE_SIZE_ATTRIBUTE, Long.toString(fileInfo.getSize()))
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName())
        final String fullPath = fileInfo.getFullPathFileName()
        if (fullPath != null) {
            final int index = fullPath.lastIndexOf("/")
            if (index > -1) {
                final String path = fullPath.substring(0, index)
                attributes.put(CoreAttributes.PATH.key(), path)
            }
        }
        return attributes
    }

}

processor = new ListSFTPCustomV2Processor()
