package janusgraph.util.batchimport.unsafe.input;


import janusgraph.util.batchimport.unsafe.input.csv.Decorator;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Common {@link InputEntityVisitor} decorators, able to provide defaults or overrides.
 */
public class InputEntityDecorators
{
    private InputEntityDecorators()
    {
    }

    /**
     * Ensures that all input nodes will at least have the given set of labels.
     */
    public static Decorator additiveLabels(final String[] labelNamesToAdd )
    {
        if ( labelNamesToAdd == null || labelNamesToAdd.length == 0 )
        {
            return NO_DECORATOR;
        }

        return node -> new AdditiveLabelsDecorator( node, labelNamesToAdd );
    }

    /**
     * Ensures that input relationships without a specified relationship type will get
     * the specified default relationship type.
     */
    public static Decorator defaultRelationshipType( final String defaultType )
    {
        return defaultType == null
                ? NO_DECORATOR
                : relationship -> new RelationshipTypeDecorator( relationship, defaultType );
    }

    private static final class AdditiveLabelsDecorator extends InputEntityVisitor.Delegate
    {
        private final String[] transport = new String[1];
        private final String[] labelNamesToAdd;
        private final boolean[] seenLabels;
        private boolean seenLabelField;

        AdditiveLabelsDecorator( InputEntityVisitor actual, String[] labelNamesToAdd )
        {
            super( actual );
            this.labelNamesToAdd = labelNamesToAdd;
            this.seenLabels = new boolean[labelNamesToAdd.length];
        }

        @Override
        public boolean labelField( long labelField )
        {
            seenLabelField = true;
            return super.labelField( labelField );
        }

        @Override
        public boolean labels( String[] labels )
        {
            if ( !seenLabelField )
            {
                for ( String label : labels )
                {
                    for ( int i = 0; i < labelNamesToAdd.length; i++ )
                    {
                        if ( !seenLabels[i] && labelNamesToAdd[i].equals( label ) )
                        {
                            seenLabels[i] = true;
                        }
                    }
                }
            }
            return super.labels( labels );
        }

        @Override
        public void endOfEntity() throws IOException
        {
            /*if ( !seenLabelField )
            {
                for ( int i = 0; i < seenLabels.length; i++ )
                {
                    if ( !seenLabels[i] )
                    {
                        transport[0] = labelNamesToAdd[i];
                        super.labels( transport );
                    }
                }
            }

            Arrays.fill( seenLabels, false );
            seenLabelField = false;*/
            super.endOfEntity();
        }
    }

    private static final class RelationshipTypeDecorator extends InputEntityVisitor.Delegate
    {
        private final String defaultType;
        private boolean hasType;

        RelationshipTypeDecorator( InputEntityVisitor actual, String defaultType )
        {
            super( actual );
            this.defaultType = defaultType;
        }

        @Override
        public boolean endId(Object id, Group group) {

            if ( !hasType )
            {
                super.type( defaultType );
                hasType = false;
            }
            return super.endId(id, group);
        }

        @Override
        public boolean type( long type )
        {
            hasType = true;
            return super.type( type );
        }

        @Override
        public boolean type( String type )
        {
            if ( type != null )
            {
                hasType = true;
            }
            return super.type( type );
        }

        @Override
        public void endOfEntity() throws IOException
        {


            super.endOfEntity();
        }
    }

    public static Decorator decorators( final Decorator... decorators )
    {
        return new Decorator()
        {
            @Override
            public InputEntityVisitor apply( InputEntityVisitor from )
            {
                for ( Decorator decorator : decorators )
                {
                    from = decorator.apply( from );
                }
                return from;
            }

            @Override
            public boolean isMutable()
            {
                return Stream.of( decorators ).anyMatch( Decorator::isMutable );
            }
        };
    }

    public static final Decorator NO_DECORATOR = value -> value;
}
