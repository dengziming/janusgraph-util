package janusgraph.util.batchimport.unsafe.input.csv;



public abstract class Value extends AnyValue
{
    @Override
    public boolean eq( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    public abstract boolean equals( Value other );

    public abstract boolean equals( byte[] x );

    public abstract boolean equals( short[] x );

    public abstract boolean equals( int[] x );

    public abstract boolean equals( long[] x );

    public abstract boolean equals( float[] x );

    public abstract boolean equals( double[] x );

    public abstract boolean equals( boolean x );

    public abstract boolean equals( boolean[] x );

    public abstract boolean equals( long x );

    public abstract boolean equals( double x );

    public abstract boolean equals( char x );

    public abstract boolean equals( String x );

    public abstract boolean equals( char[] x );

    public abstract boolean equals( String[] x );


    public boolean isNaN()
    {
        return false;
    }
}
