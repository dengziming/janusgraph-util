package janusgraph.util.batchimport.unsafe;

import janusgraph.util.batchimport.unsafe.helps.InputException;
import janusgraph.util.batchimport.unsafe.helps.Predicates;
import janusgraph.util.batchimport.unsafe.helps.collection.PrefetchingIterator;
import janusgraph.util.batchimport.unsafe.input.csv.Type;
import janusgraph.util.batchimport.unsafe.io.fs.FileSystemImpl;
import janusgraph.util.batchimport.unsafe.io.fs.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import static janusgraph.util.batchimport.unsafe.Configuration.BAD_FILE_NAME;
import static janusgraph.util.batchimport.unsafe.helps.ArrayUtil.join;
import static janusgraph.util.batchimport.unsafe.helps.Exceptions.withMessage;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.repeat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import janusgraph.util.batchimport.unsafe.input.csv.Configuration;

public class BulkLoadTest
{
    private static final int MAX_LABEL_ID = 4;
    private static final int RELATIONSHIP_COUNT = 10_000;
    private static final int NODE_COUNT = 100;
    private static final IntPredicate TRUE = i -> true;
    private static final IntPredicate FALSE = i -> false;

    
    private final FileSystemImpl fileSystem = new FileSystemImpl();
    private File testDirectory = new File("target/test");
    private static final String node_label = "NODE";
    private static final String relationship_label = "RELATIONSHIP";

    /**
     * Delimiter used between files in an input group.
     */
    private static final String MULTI_FILE_DELIMITER = ",";
    private static final String DATABASE_DIRECTORY = "graph-db";


    private final Random random = new Random();

    private int dataIndex;


    public File directory( String name )
    {
        File dir = new File( testDirectory, name );
        if ( !fileSystem.fileExists( dir ) )
        {
            fileSystem.mkdir( dir );
        }
        return dir;
    }
    
    
    private File graphDbDir()
    {
        return directory( DATABASE_DIRECTORY );
    }

    private void ensureExits(File dir)
    {

        if ( fileSystem.fileExists( dir ) && !fileSystem.isDirectory( dir ) )
        {
            throw new IllegalStateException( dir + " exists and is not a directory!" );
        }

        try
        {
            fileSystem.mkdirs( dir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void delete(File dir)
    {

        fileSystem.deleteFile( dir );
    }

    @Before
    public void ensureExits(){
        ensureExits(graphDbDir());
    }

    @After
    public void delete(){
        delete(graphDbDir());
    }




    @Test
    public void shouldImportWithAsManyDefaultsAsAvailable() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        importTool(
                "--into",
                graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label,
                nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--edges:" + relationship_label,
                relationshipData( true, config, nodeIds, TRUE).getAbsolutePath() );

    }

    @Test
    public void shouldImportWithHeadersBeingInSeparateFiles() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--delimiter", "TAB",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes:" + node_label,
                nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                nodeData( false, config, nodeIds, TRUE ).getAbsolutePath(),
                "--edges:" + relationship_label,
                relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                relationshipData( false, config, nodeIds, TRUE).getAbsolutePath() );

    }

    @Test
    public void shouldIgnoreWhitespaceAroundIntegers() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        List<String> values = Arrays.asList( "17", "    21", "99   ", "  34  ", "-34", "        -12", "-92 " );

        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,name,s:short,b:byte,i:int,l:long,f:float,d:double" );

            // For each test value
            for ( String value : values )
            {
                // Save value as a String in name
                writer.print( "PERSON,'" + value + "'" );
                // For each numerical type
                for ( int j = 0; j < 6; j++ )
                {
                    writer.print( "," + value );
                }
                // End line
                writer.println();
            }
        }

        // WHEN
        importTool( "--into", graphDbDir().getAbsolutePath(), "--quote", "'", "--nodes:" + node_label, data.getAbsolutePath() );
    }

    @Test
    public void shouldIgnoreWhitespaceAroundDecimalNumbers() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        List<String> values = Arrays.asList( "1.0", "   3.5", "45.153    ", "   925.12   ", "-2.121", "   -3.745",
                "-412.153    ", "   -5.12   " );

        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,name,f:float,d:double" );

            // For each test value
            for ( String value : values )
            {
                // Save value as a String in name
                writer.print( "PERSON,'" + value + "'" );
                // For each numerical type
                for ( int j = 0; j < 2; j++ )
                {
                    writer.print( "," + value );
                }
                // End line
                writer.println();
            }
        }

        // WHEN
        importTool( "--into", graphDbDir().getAbsolutePath(), "--quote", "'", "--nodes:" + node_label, data.getAbsolutePath() );
    }

    @Test
    public void shouldIgnoreWhitespaceAroundBooleans() throws Exception
    {
        // GIVEN
        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.println( ":LABEL,name,adult:boolean" );

            writer.println( "PERSON,'t1',true" );
            writer.println( "PERSON,'t2',  true" );
            writer.println( "PERSON,'t3',true  " );
            writer.println( "PERSON,'t4',  true  " );

            writer.println( "PERSON,'f1',false" );
            writer.println( "PERSON,'f2',  false" );
            writer.println( "PERSON,'f3',false  " );
            writer.println( "PERSON,'f4',  false  " );
            writer.println( "PERSON,'f5',  truebutactuallyfalse  " );

            writer.println( "PERSON,'f6',  non true things are interpreted as false  " );
        }

        // WHEN
        importTool( "--into", graphDbDir().getAbsolutePath(), "--quote", "'", "--nodes:" + node_label, data.getAbsolutePath() );

        // THEN
    }

    @Test
    public void shouldIgnoreWhitespaceInAndAroundIntegerArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values = new String[]{ "   17", "21", "99   ", "  34  ", "-34", "        -12", "-92 " };

        File data = writeArrayCsv(
                new String[]{ "s:short[]", "b:byte[]", "i:int[]", "l:long[]", "f:float[]", "d:double[]" }, values );

        // WHEN
        importTool( "--into", graphDbDir().getAbsolutePath(), "--quote", "'", "--nodes:" + node_label, data.getAbsolutePath() );
    }

    @Test
    public void shouldIgnoreWhitespaceInAndAroundDecimalArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values =
                new String[]{ "1.0", "   3.5", "45.153    ", "   925.12   ", "-2.121", "   -3.745", "-412.153    ",
                        "   -5.12   " };

        File data = writeArrayCsv( new String[]{ "f:float[]", "d:double[]" }, values );

        // WHEN
        importTool( "--into", graphDbDir().getAbsolutePath(), "--quote", "'", "--nodes:" + node_label, data.getAbsolutePath() );

    }

    @Test
    public void shouldIgnoreWhitespaceInAndAroundBooleanArrays() throws Exception
    {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values =
                new String[]{ "true", "  true", "true   ", "  true  ", " false ", "false ", " false", "false ",
                        " false" };
        String expected = joinStringArray( values );

        File data = writeArrayCsv( new String[]{ "b:boolean[]" }, values );

        // WHEN
        importTool( "--into", graphDbDir().getAbsolutePath(), "--quote", "'", "--nodes:" + node_label, data.getAbsolutePath() );

    }

    @Test
    public void shouldFailIfHeaderHasLessColumnsThanData() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;

        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--delimiter", "TAB",
                    "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                    "--nodes:" + node_label, nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                            nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns )
                                    .getAbsolutePath(),
                    "--edges:" + relationship_label, relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                            relationshipData( false, config, nodeIds, TRUE).getAbsolutePath() );

            fail( "Should have thrown exception" );
        }
        catch ( Exception e )
        {
            // assertTrue( e.getMessage().contains( "Extra column not present in header on line" ) );
        }


    }

    @Test
    public void shouldWarnIfHeaderHasLessColumnsThanDataWhenToldTo() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;
        File bad = badFile();

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--bad", bad.getAbsolutePath(),
                "--bad-tolerance", Integer.toString( nodeIds.size() * extraColumns ),
                "--ignore-extra-columns",
                "--delimiter", "TAB",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes:" + node_label, nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                        nodeData( false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns )
                                .getAbsolutePath(),
                "--edges:" + relationship_label, relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                        relationshipData( false, config, nodeIds, TRUE).getAbsolutePath() );

        // THEN
        String badContents = FileUtils.readTextFile( bad, Charset.defaultCharset() );
        assertTrue( badContents.contains( "Extra column not present in header on line" ) );
    }

    @Test
    public void shouldImportSplitInputFiles() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, // One group with one header file and one data file
                nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                nodeData( false, config, nodeIds, lines( 0, NODE_COUNT / 2 ) ).getAbsolutePath(),
                "--nodes:" + node_label, // One group with two data files, where the header sits in the first file
                nodeData( true, config, nodeIds,
                        lines( NODE_COUNT / 2, NODE_COUNT * 3 / 4 ) ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                nodeData( false, config, nodeIds, lines( NODE_COUNT * 3 / 4, NODE_COUNT ) ).getAbsolutePath(),
                "--edges:" + relationship_label,
                relationshipHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                relationshipData( false, config, nodeIds, TRUE).getAbsolutePath() );
    }

    @Test
    public void shouldImportMultipleInputsWithAddedLabelsAndDefaultRelationshipType() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final String[] firstLabels = {"AddedOne", "AddedTwo"};
        final String[] secondLabels = {"AddedThree"};
        final String firstType = "TYPE_1";
        final String secondType = "TYPE_2";

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label + join( firstLabels, ":" ),
                nodeData( true, config, nodeIds, lines( 0, NODE_COUNT / 2 ) ).getAbsolutePath(),
                "--nodes:" + node_label + join( secondLabels, ":" ),
                nodeData( true, config, nodeIds, lines( NODE_COUNT / 2, NODE_COUNT ) ).getAbsolutePath(),
                "--edges:" + relationship_label + firstType,
                relationshipData( true, config, nodeIds, lines( 0, RELATIONSHIP_COUNT / 2 )).getAbsolutePath(),
                "--edges:" + relationship_label + secondType,
                relationshipData( true, config, nodeIds,
                        lines( RELATIONSHIP_COUNT / 2, RELATIONSHIP_COUNT )).getAbsolutePath() );

        // THEN
        MutableInt numberOfNodesWithFirstSetOfLabels = new MutableInt();
        MutableInt numberOfNodesWithSecondSetOfLabels = new MutableInt();
        MutableInt numberOfRelationshipsWithFirstType = new MutableInt();
        MutableInt numberOfRelationshipsWithSecondType = new MutableInt();
    }


    @Test
    public void shouldImportGroupsOfOverlappingIds() throws Exception
    {
        // GIVEN
        List<String> groupOneNodeIds = asList( "1", "2", "3" );
        List<String> groupTwoNodeIds = asList( "4", "5", "2" );
        List<RelationshipDataLine> rels = asList(
                relationship( "1", "4", "TYPE" ),
                relationship( "2", "5", "TYPE" ),
                relationship( "3", "2", "TYPE" ) );
        Configuration config = Configuration.COMMAS;
        String groupOne = "Actor";
        String groupTwo = "Movie";

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeHeader( config, groupOne ) + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupOneNodeIds, TRUE ),
                "--nodes:" + node_label, nodeHeader( config, groupTwo ) + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupTwoNodeIds, TRUE ),
                "--edges:" + relationship_label, relationshipHeader( config, groupOne, groupTwo, false ) + MULTI_FILE_DELIMITER +
                                   relationshipData( false, config, rels.iterator(), TRUE) );

    }

    @Test
    public void shouldBeAbleToMixSpecifiedAndUnspecifiedGroups() throws Exception
    {
        // GIVEN
        List<String> groupOneNodeIds = asList( "1", "2", "3" );
        List<String> groupTwoNodeIds = asList( "4", "5", "2" );
        Configuration config = Configuration.COMMAS;

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeHeader( config, "MyGroup" ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupOneNodeIds, TRUE ).getAbsolutePath(),
                "--nodes:" + node_label, nodeHeader( config ).getAbsolutePath() + MULTI_FILE_DELIMITER +
                           nodeData( false, config, groupTwoNodeIds, TRUE ).getAbsolutePath() );

        // THEN
    }

    @Test
    public void shouldImportWithoutTypeSpecifiedInRelationshipHeaderbutWithDefaultTypeInArgument() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        String type = randomType();

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                // there will be no :TYPE specified in the header of the edges below
                "--edges:" + relationship_label + type,
                relationshipData( true, config, nodeIds, TRUE).getAbsolutePath() );

        // THEN
    }

    @Test
    public void shouldIncludeSourceInformationInNodeIdCollisionError() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c", "d", "e", "f", "a", "g" );
        Configuration config = Configuration.COMMAS;
        File nodeHeaderFile = nodeHeader( config );
        File nodeData1 = nodeData( false, config, nodeIds, lines( 0, 4 ) );
        File nodeData2 = nodeData( false, config, nodeIds, lines( 4, nodeIds.size() ) );

        // WHEN
        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--nodes:" + node_label, nodeHeaderFile.getAbsolutePath() + MULTI_FILE_DELIMITER +
                               nodeData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                               nodeData2.getAbsolutePath() );
            fail( "Should have failed with duplicate node IDs" );
        }
        catch ( Exception e )
        {
            // THEN
            // assertExceptionContains( e, "'a' is defined more than once", DuplicateInputIdException.class );
        }
    }

    @Test
    public void shouldSkipDuplicateNodesIfToldTo() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c", "d", "e", "f", "a", "g" );
        Configuration config = Configuration.COMMAS;
        File nodeHeaderFile = nodeHeader( config );
        File nodeData1 = nodeData( false, config, nodeIds, lines( 0, 4 ) );
        File nodeData2 = nodeData( false, config, nodeIds, lines( 4, nodeIds.size() ) );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--skip-duplicate-nodes",
                "--nodes:" + node_label, nodeHeaderFile.getAbsolutePath() + MULTI_FILE_DELIMITER +
                           nodeData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                           nodeData2.getAbsolutePath() );

    }

    @Test
    public void shouldLogRelationshipsReferringToMissingNode() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, TRUE );
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE", "aa" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE", "bb" ), //      line 3 of file1
                relationship( "b", "c", "KNOWS", "cc" ), //         line 1 of file2
                relationship( "c", "a", "KNOWS", "dd" ), //         line 2 of file2
                relationship( "missing", "a", "KNOWS", "ee" ) ); // line 3 of file2
        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ));
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ));
        File bad = badFile();

        // WHEN importing data where some edges refer to missing nodes
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData.getAbsolutePath(),
                "--bad", bad.getAbsolutePath(),
                "--bad-tolerance", "2",
                "--edges:" + relationship_label, relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                                   relationshipData2.getAbsolutePath() );

        // THEN
        String badContents = FileUtils.readTextFile( bad, Charset.defaultCharset() );
        assertTrue( "Didn't contain first bad relationship", badContents.contains( "bogus" ) );
        assertTrue( "Didn't contain second bad relationship", badContents.contains( "missing" ) );
    }

    @Test
    public void skipLoggingOfBadEntries() throws Exception
    {
        badFile().delete();
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, TRUE );
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship( "a", "b", "TYPE", "aa" ), //          line 2 of file1
                relationship( "c", "bogus", "TYPE", "bb" ), //      line 3 of file1
                relationship( "b", "c", "KNOWS", "cc" ), //         line 1 of file2
                relationship( "c", "a", "KNOWS", "dd" ), //         line 2 of file2
                relationship( "missing", "a", "KNOWS", "ee" ) ); // line 3 of file2
        File relationshipData1 = relationshipData( true, config, relationships.iterator(), lines( 0, 2 ));
        File relationshipData2 = relationshipData( false, config, relationships.iterator(), lines( 2, 5 ));

        // WHEN importing data where some edges refer to missing nodes
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData.getAbsolutePath(),
                "--bad-tolerance", "2",
                "--skip-bad-entries-logging", "true",
                "--edges:" + relationship_label, relationshipData1.getAbsolutePath() + MULTI_FILE_DELIMITER +
                        relationshipData2.getAbsolutePath() );

        assertFalse( badFile().exists() );
    }


    @Test
    public void shouldImportFromInputDataEncodedWithSpecificCharset() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        Charset charset = Charset.forName( "UTF-16" );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--input-encoding", charset.name(),
                "--nodes:" + node_label, nodeData( true, config, nodeIds, TRUE, charset ).getAbsolutePath(),
                "--edges:" + relationship_label, relationshipData( true, config, nodeIds, TRUE, true, charset )
                        .getAbsolutePath() );
    }

    @Test
    public void shouldDisallowImportWithoutNodesInput() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--edges:" + relationship_label,
                    relationshipData( true, config, nodeIds, TRUE).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "No node input" ) );
        }
    }

    @Test
    public void shouldBeAbleToImportAnonymousNodes() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "1", "", "", "", "3", "", "", "", "", "", "5" );
        Configuration config = Configuration.COMMAS;
        List<RelationshipDataLine> relationshipData = asList( relationship( "1", "3", "KNOWS" ) );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--edges:" + relationship_label, relationshipData( true, config, relationshipData.iterator(),
                        TRUE).getAbsolutePath() );


    }

    @Test
    public void shouldNotTrimStringsByDefault() throws Exception
    {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        File data = data( "id:ID,name", "1,\"" + name + "\"");

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath() );
    }

    @Test
    public void shouldTrimStringsIfConfiguredTo() throws Exception
    {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        File data = data(
                "id:ID,name",
                "1,\"" + name + "\"",
                "2," + name );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath(),
                "--trim-strings", "true" );
    }


    @Test
    public void shouldCollectUnlimitedNumberOfBadEntries() throws Exception
    {
        // GIVEN
        List<String> nodeIds = Collections.nCopies( 10_000, "A" );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--skip-duplicate-nodes",
                "--bad-tolerance", "true" );

        // THEN
        // all those duplicates should just be accepted using the - for specifying bad tolerance
    }


    @Test
    public void shouldAllowMultilineFieldsWhenEnabled() throws Exception
    {
        // GIVEN
        File data = data( "id:ID,name", "1,\"This is a line with\nnewlines in\"" );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath(),
                "--multiline-fields", "true" );
    }

    @Test
    public void shouldSkipEmptyFiles() throws Exception
    {
        // GIVEN
        File data = data( "" );

        // WHEN
        importTool( "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath() );
    }

    @Test
    public void shouldIgnoreEmptyQuotedStringsIfConfiguredTo() throws Exception
    {
        // GIVEN
        File data = data(
                "id:ID,one,two,three",
                "1,\"\",,value" );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath(),
                "--ignore-empty-strings", "true" );
    }

    @Test
    public void shouldPrintUserFriendlyMessageAboutUnsupportedMultilineFields() throws Exception
    {
        // GIVEN
        File data = data(
                "id:ID,name",
                "1,\"one\ntwo\nthree\"",
                "2,four" );

        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--nodes:" + node_label, data.getAbsolutePath(),
                    "--multiline-fields", "false" );
        }
        catch ( InputException e )
        {
            fail( "Should have success" );
        }
    }

    @Test
    public void shouldAcceptRawAsciiCharacterCodeAsQuoteConfiguration() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                "id:ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath(),
                "--quote", String.valueOf( weirdDelimiter ) );

    }

    @Test
    public void shouldAcceptSpecialTabCharacterAsDelimiterConfiguration() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--delimiter", "\\t",
                "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                "--nodes:" + node_label, nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                "--edges:" + relationship_label, relationshipData( true, config, nodeIds, TRUE).getAbsolutePath() );

    }

    @Test
    public void shouldReportBadDelimiterConfiguration() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--delimiter", "\\bogus",
                    "--array-delimiter", String.valueOf( config.arrayDelimiter() ),
                    "--nodes:" + node_label, nodeData( true, config, nodeIds, TRUE ).getAbsolutePath(),
                    "--edges:" + relationship_label, relationshipData( true, config, nodeIds, TRUE).getAbsolutePath() );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "bogus" ) );
        }
    }

    @Test
    public void shouldFailAndReportStartingLineForUnbalancedQuoteInMiddle() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--nodes:" + node_label, nodeDataWithMissingQuote( 2 * unbalancedStartLine, unbalancedStartLine )
                            .getAbsolutePath() );
        }
        catch ( InputException e )
        {
            // THEN
            fail( "Should have success" );
        }
    }

    @Test
    public void shouldAcceptRawEscapedAsciiCodeAsQuoteConfiguration() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                "id:ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath(),
                "--quote", "\\1" );
    }

    @Test
    public void shouldFailAndReportStartingLineForUnbalancedQuoteAtEnd() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--nodes:" + node_label, nodeDataWithMissingQuote( unbalancedStartLine, unbalancedStartLine ).getAbsolutePath() );
        }
        catch ( InputException e )
        {
            // THEN
            //
            fail( "Should have success" );
            // assertThat( e.getMessage(), containsString("Multi-line fields") );
        }
    }

    @Test
    public void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration1() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 126; // 126 ~ (tilde)
        String weirdStringDelimiter = "\\126";
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                "id:ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN given as raw ascii
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath(),
                "--quote", weirdStringDelimiter );

        // THEN
        assertEquals( "~", "" + weirdDelimiter );
        assertEquals( "~".charAt( 0 ), weirdDelimiter );

    }

    @Test
    public void shouldFailOnUnbalancedQuoteWithMultilinesEnabled() throws Exception
    {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--multiline-fields", "true",
                    "--nodes:" + node_label,
                    nodeDataWithMissingQuote( 2 * unbalancedStartLine, unbalancedStartLine ).getAbsolutePath() );
        }
        catch ( InputException e )
        {
            fail( "Should have success" );
        }
    }

    private File nodeDataWithMissingQuote( int totalLines, int unbalancedStartLine ) throws Exception
    {
        String[] lines = new String[totalLines + 1];

        lines[0] = "id:ID,NAME";

        for ( int i = 1; i <= totalLines; i++ )
        {
            StringBuilder line = new StringBuilder( String.format( "%d,", i ) );
            if ( i == unbalancedStartLine )
            {
                // Missing the end quote
                line.append( "\"Secret Agent" );
            }
            else
            {
                line.append( "Agent" );
            }
            lines[i] = line.toString();
        }

        return data( lines );
    }

    @Test
    public void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration2() throws Exception
    {
        // GIVEN
        char weirdDelimiter = 126; // 126 ~ (tilde)
        String weirdStringDelimiter = "~";
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        File data = data(
                "id:ID,name",
                "1," + name1,
                "2," + name2 );

        // WHEN given as string
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, data.getAbsolutePath(),
                "--quote", weirdStringDelimiter );

        // THEN
        assertEquals( weirdStringDelimiter, "" + weirdDelimiter );
        assertEquals( weirdStringDelimiter.charAt( 0 ), weirdDelimiter );

    }

    @Test
    public void shouldRespectDbConfig() throws Exception
    {
        // GIVEN
        int arrayBlockSize = 10;
        int stringBlockSize = 12;
        File dbConfig = file( "db.properties" );

        List<String> nodeIds = nodeIds();

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--db-config", dbConfig.getAbsolutePath(),
                "--nodes:" + node_label, nodeData( true, Configuration.COMMAS, nodeIds, value -> true ).getAbsolutePath() );
    }

    @Test
    public void useProvidedAdditionalConfig() throws Exception
    {
        // GIVEN
        int arrayBlockSize = 10;
        int stringBlockSize = 12;
        File dbConfig = file( "db.properties" );

        List<String> nodeIds = nodeIds();

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--additional-config", dbConfig.getAbsolutePath(),
                "--nodes:" + node_label, nodeData( true, Configuration.COMMAS, nodeIds, value -> true ).getAbsolutePath() );

    }


    @Test
    public void shouldRespectBufferSizeSetting() throws Exception
    {
        // GIVEN
        List<String> lines = new ArrayList<>();
        lines.add( "id:ID,name,:LABEL" );
        lines.add( "id," + repeat( 'l', 2_000 ) + ",Person" );

        // WHEN
        try
        {
            importTool(
                    "--into", graphDbDir().getAbsolutePath(),
                    "--nodes:" + node_label, data( lines.toArray( new String[lines.size()] ) ).getAbsolutePath(),
                    "--read-buffer-size", "1k"
                    );
            fail( "Should've failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "input data" ) );
        }
    }

    @Test
    public void shouldRespectMaxMemoryPercentageSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--max-memory", "60%" );
    }

    @Test
    public void shouldFailOnInvalidMaxMemoryPercentageSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        try
        {
            // WHEN
            importTool( "--into", graphDbDir().getAbsolutePath(), "--nodes:" + node_label,
                    nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(), "--max-memory", "110%" );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "percent" ) );
        }
    }

    @Test
    public void shouldRespectMaxMemorySuffixedSetting() throws Exception
    {
        // GIVEN
        List<String> nodeIds = nodeIds( 10 );

        // WHEN
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData( true, Configuration.COMMAS, nodeIds, TRUE ).getAbsolutePath(),
                "--max-memory", "100M" );
    }

    @Test
    public void shouldTreatRelationshipWithMissingStartOrEndIdOrTypeAsBadRelationship() throws Exception
    {
        // GIVEN
        List<String> nodeIds = asList( "a", "b", "c" );
        Configuration config = Configuration.COMMAS;
        File nodeData = nodeData( true, config, nodeIds, TRUE );

        List<RelationshipDataLine> relationships = Arrays.asList(
                relationship( "a", null ),
                relationship( null, "b" ),
                relationship( "a", "b" ) );

        File relationshipData = relationshipData( true, config, relationships.iterator(), TRUE);
        File bad = badFile();

        // WHEN importing data where some edges refer to missing nodes
        importTool(
                "--into", graphDbDir().getAbsolutePath(),
                "--nodes:" + node_label, nodeData.getAbsolutePath(),
                "--bad", bad.getAbsolutePath(),
                "--skip-bad-edges", "true",
                "--edges:" + relationship_label, relationshipData.getAbsolutePath() );

        String badContents = FileUtils.readTextFile( bad, Charset.defaultCharset() );
        assertEquals( badContents, 2, occurencesOf( badContents, "is missing data" ) );
    }


    private static int occurencesOf( String text, String lookFor )
    {
        int index = -1;
        int count = -1;
        do
        {
            count++;
            index = text.indexOf( lookFor, index + 1 );
        }
        while ( index != -1 );
        return count;
    }

    private File writeArrayCsv( String[] headers, String[] values ) throws FileNotFoundException
    {
        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.print( ":LABEL" );
            for ( String header : headers )
            {
                writer.print( "," + header );
            }
            // End line
            writer.println();

            // Save value as a String in name
            writer.print( "PERSON" );
            // For each type
            for ( String ignored : headers )
            {
                boolean comma = true;
                for ( String value : values )
                {
                    if ( comma )
                    {
                        writer.print( "," );
                        comma = false;
                    }
                    else
                    {
                        writer.print( ";" );
                    }
                    writer.print( value );
                }
            }
            // End line
            writer.println();
        }
        return data;
    }

    private String joinStringArray( String[] values )
    {
        return Arrays.stream( values ).map( String::trim ).collect( joining( ", ", "[", "]" ) );
    }

    private File data( String... lines ) throws Exception
    {
        File file = file( fileName( "data.csv" ) );
        try ( PrintStream writer = writer( file, Charset.defaultCharset() ) )
        {
            for ( String line : lines )
            {
                writer.println( line );
            }
        }
        return file;
    }


    private List<String> nodeIds()
    {
        return nodeIds( NODE_COUNT );
    }

    private List<String> nodeIds( int count )
    {
        List<String> ids = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            ids.add( randomNodeId() );
        }
        return ids;
    }

    private String randomNodeId()
    {
        return UUID.randomUUID().toString();
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate ) throws Exception
    {
        return nodeData( includeHeader, config, nodeIds, linePredicate, Charset.defaultCharset() );
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, Charset encoding ) throws Exception
    {
        return nodeData( includeHeader, config, nodeIds, linePredicate, encoding, 0 );
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, Charset encoding, int extraColumns ) throws Exception
    {
        File file = file( fileName( "nodes.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            if ( includeHeader )
            {
                writeNodeHeader( writer, config, null );
            }
            writeNodeData( writer, config, nodeIds, linePredicate, extraColumns );
        }
        return file;
    }

    private PrintStream writer( File file, Charset encoding ) throws Exception
    {
        return new PrintStream( file, encoding.name() );
    }

    private File nodeHeader( Configuration config ) throws Exception
    {
        return nodeHeader( config, null );
    }

    private File nodeHeader( Configuration config, String idGroup ) throws Exception
    {
        return nodeHeader( config, idGroup, Charset.defaultCharset() );
    }

    private File nodeHeader( Configuration config, String idGroup, Charset encoding ) throws Exception
    {
        File file = file( fileName( "nodes-header.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            writeNodeHeader( writer, config, idGroup );
        }
        return file;
    }

    private void writeNodeHeader( PrintStream writer, Configuration config, String idGroup )
    {
        char delimiter = config.delimiter();
        writer.println( idEntry( "id", Type.ID, idGroup ) + delimiter + "name" /*+ delimiter + "labels:LABEL"*/ );
    }

    private String idEntry( String name, Type type, String idGroup )
    {
        return (name != null ? name : "") + ":" + type.name() + (idGroup != null ? "(" + idGroup + ")" : "");
    }

    private void writeNodeData( PrintStream writer, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, int extraColumns )
    {
        char delimiter = config.delimiter();
        char arrayDelimiter = config.arrayDelimiter();
        for ( int i = 0; i < nodeIds.size(); i++ )
        {
            if ( linePredicate.test( i ) )
            {
                writer.println( getLine( nodeIds.get( i ), delimiter, arrayDelimiter, extraColumns ) );
            }
        }
    }

    private String getLine( String nodeId, char delimiter, char arrayDelimiter, int extraColumns )
    {
        StringBuilder stringBuilder = new StringBuilder().append( nodeId ).append( delimiter ).append( randomName() )
                /*.append( delimiter ).append( randomLabels( arrayDelimiter ) )*/;

        for ( int i = 0; i < extraColumns; i++ )
        {
            stringBuilder.append( delimiter ).append( "ExtraColumn" ).append( i );
        }

        return stringBuilder.toString();
    }

    private String randomLabels( char arrayDelimiter )
    {
        int length = random.nextInt( 3 );
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( arrayDelimiter );
            }
            builder.append( labelName( random.nextInt( MAX_LABEL_ID ) ) );
        }
        return builder.toString();
    }

    private String labelName( int number )
    {
        return "LABEL_" + number;
    }

    private String randomName()
    {
        int length = random.nextInt( 10 ) + 5;
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            builder.append( (char) ('a' + random.nextInt( 20 )) );
        }
        return builder.toString();
    }

    private File relationshipData(boolean includeHeader, Configuration config, List<String> nodeIds,
                                  IntPredicate linePredicate) throws Exception
    {
        return relationshipData( includeHeader, config, nodeIds, linePredicate, false, Charset.defaultCharset() );
    }

    private File relationshipData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, boolean specifyType, Charset encoding ) throws Exception
    {
        return relationshipData( includeHeader, config, randomRelationships( nodeIds ), linePredicate,
                encoding );
    }

    private File relationshipData(boolean includeHeader, Configuration config,
                                  Iterator<RelationshipDataLine> data, IntPredicate linePredicate) throws Exception
    {
        return relationshipData( includeHeader, config, data, linePredicate, Charset.defaultCharset() );
    }

    private File relationshipData(boolean includeHeader, Configuration config,
                                  Iterator<RelationshipDataLine> data, IntPredicate linePredicate,
                                  Charset encoding) throws Exception
    {
        File file = file( fileName( "edges.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            if ( includeHeader )
            {
                writeRelationshipHeader( writer, config, null, null, false );
            }
            writeRelationshipData( writer, config, data, linePredicate, false );
        }
        return file;
    }

    private File relationshipHeader( Configuration config ) throws Exception
    {
        return relationshipHeader( config, Charset.defaultCharset() );
    }

    private File relationshipHeader( Configuration config, Charset encoding ) throws Exception
    {
        return relationshipHeader( config, null, null, false, encoding );
    }

    private File relationshipHeader( Configuration config, String startIdGroup, String endIdGroup, boolean specifyType )
            throws Exception
    {
        return relationshipHeader( config, startIdGroup, endIdGroup, specifyType, Charset.defaultCharset() );
    }

    private File relationshipHeader( Configuration config, String startIdGroup, String endIdGroup, boolean specifyType,
            Charset encoding ) throws Exception
    {
        File file = file( fileName( "edges-header.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            writeRelationshipHeader( writer, config, startIdGroup, endIdGroup, specifyType );
        }
        return file;
    }

    private String fileName( String name )
    {
        return dataIndex++ + "-" + name;
    }

    private File file( String localname )
    {
        return new File( graphDbDir(), localname );
    }

    private File badFile()
    {
        return new File( graphDbDir(), BAD_FILE_NAME );
    }

    private void writeRelationshipHeader( PrintStream writer, Configuration config,
            String startIdGroup, String endIdGroup, boolean specifyType )
    {
        char delimiter = config.delimiter();
        writer.println(
                idEntry( null, Type.START_ID, startIdGroup ) + delimiter +
                idEntry( null, Type.END_ID, endIdGroup ) +
                (specifyType ? (delimiter + ":" + Type.TYPE) : "") +
                delimiter + "created:long" +
                delimiter + "name:String" );
    }

    private static class RelationshipDataLine
    {
        private final String startNodeId;
        private final String endNodeId;
        private final String type;
        private final String name;

        RelationshipDataLine( String startNodeId, String endNodeId, String type, String name )
        {
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
            this.type = type;
            this.name = name;
        }

        @Override
        public String toString()
        {
            return "RelationshipDataLine [startNodeId=" + startNodeId + ", endNodeId=" + endNodeId + ", type=" + type
                   + ", name=" + name + "]";
        }
    }

    private static RelationshipDataLine relationship( String startNodeId, String endNodeId )
    {
        return relationship( startNodeId, endNodeId, null, null );
    }

    private static RelationshipDataLine relationship( String startNodeId, String endNodeId, String type )
    {
        return relationship( startNodeId, endNodeId, type, null );
    }

    private static RelationshipDataLine relationship( String startNodeId, String endNodeId, String type, String name )
    {
        return new RelationshipDataLine( startNodeId, endNodeId, type, name );
    }

    private void writeRelationshipData( PrintStream writer, Configuration config,
            Iterator<RelationshipDataLine> data, IntPredicate linePredicate, boolean specifyType )
    {
        char delimiter = config.delimiter();
        for ( int i = 0; i < RELATIONSHIP_COUNT; i++ )
        {
            if ( !data.hasNext() )
            {
                break;
            }
            RelationshipDataLine entry = data.next();
            if ( linePredicate.test( i ) )
            {
                writer.println( nullSafeString( entry.startNodeId ) +
                                delimiter + nullSafeString( entry.endNodeId ) +
                                (specifyType ? (delimiter + nullSafeString( entry.type )) : "") +
                                delimiter + currentTimeMillis() +
                                delimiter + (entry.name != null ? entry.name : "")
                );
            }
        }
    }

    private static String nullSafeString( String endNodeId )
    {
        return endNodeId != null ? endNodeId : "";
    }

    private Iterator<RelationshipDataLine> randomRelationships( final List<String> nodeIds )
    {
        return new PrefetchingIterator<RelationshipDataLine>()
        {
            @Override
            protected RelationshipDataLine fetchNextOrNull()
            {
                return new RelationshipDataLine(
                        nodeIds.get( random.nextInt( nodeIds.size() ) ),
                        nodeIds.get( random.nextInt( nodeIds.size() ) ),
                        randomType(),
                        null );
            }
        };
    }

    private static void assertExceptionContains(Exception e, String message, Class<? extends Exception> type)
            throws Exception
    {
        if ( !contains( e, message, type ) )
        {   // Rethrow the exception since we'd like to see what it was instead
            throw withMessage( e,
                    format( "Expected exception to contain cause '%s', %s. but was %s", message, type, e ) );
        }
    }

    public static boolean contains( final Throwable cause, final String containsMessage, final Class... anyOfTheseClasses )
    {
        final Predicate<Throwable> anyOfClasses = Predicates.instanceOfAny( anyOfTheseClasses );
        return contains( cause, item -> item.getMessage() != null && item.getMessage().contains( containsMessage ) &&
                anyOfClasses.test( item ) );
    }

    public static boolean contains( Throwable cause, Predicate<Throwable> toLookFor )
    {
        while ( cause != null )
        {
            if ( toLookFor.test( cause ) )
            {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private String randomType()
    {
        return "TYPE_" + random.nextInt( 4 );
    }

    private IntPredicate lines( final int startingAt, final int endingAt /*excluded*/ )
    {
        return line -> line >= startingAt && line < endingAt;
    }

    private static void importTool(String... arguments) throws Exception
    {
        String[] args = new String[arguments.length + 4];
        args[0] = "--janus-config:storage.backend";
        args[1] = "cassandra";
        args[2] = "--janus-config:storage.hostname";
        args[3] = "127.0.0.1";

        System.arraycopy(arguments, 0, args, 4, arguments.length);

        BulkLoad.main( args );
    }
}
