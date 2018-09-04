package janusgraph.util.batchimport.unsafe.helps;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings( "unchecked" )
public class Dependencies extends DependencyResolver.Adapter implements DependencySatisfier
{
    private final Supplier<DependencyResolver> parent;
    private final Map<Class<?>, List<?>> typeDependencies = new HashMap<>();

    public Dependencies()
    {
        parent = null;
    }

    public Dependencies(final DependencyResolver parent )
    {
        this.parent = () -> parent;
    }

    public Dependencies(Supplier<DependencyResolver> parent )
    {
        this.parent = parent;
    }

    @Override
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
    {
        List<?> options = typeDependencies.get( type );

        if ( options != null )
        {
            return selector.select( type, (Iterable<T>) options);
        }

        // Try parent
        if ( parent != null )
        {
            DependencyResolver dependencyResolver = parent.get();

            if ( dependencyResolver != null )
            {
                return dependencyResolver.resolveDependency( type, selector );
            }
        }

        // Out of options
        throw new UnsatisfiedDependencyException( type );
    }

    @Override
    public <T> Supplier<T> provideDependency( final Class<T> type, final SelectionStrategy selector )
    {
        return () -> resolveDependency( type, selector );
    }

    @Override
    public <T> Supplier<T> provideDependency( final Class<T> type )
    {
        return () -> resolveDependency( type );
    }

    @Override
    public <T> T satisfyDependency( T dependency )
    {
        // File this object under all its possible types
        Class<?> type = dependency.getClass();
        do
        {
            List<Object> deps = (List<Object>) typeDependencies.get( type );
            if ( deps == null )
            {
                deps = new ArrayList<>(  );
                typeDependencies.put(type, deps);
            }
            deps.add( dependency );

            // Add as all interfaces
            Class<?>[] interfaces = type.getInterfaces();
            addInterfaces(interfaces, dependency);

            type = type.getSuperclass();
        }
        while ( type != null );

        return dependency;
    }

    public void satisfyDependencies( Object... dependencies )
    {
        for ( Object dependency : dependencies )
        {
            satisfyDependency( dependency );
        }
    }

    private <T> void addInterfaces( Class<?>[] interfaces, T dependency )
    {
        for ( Class<?> type : interfaces )
        {
            List<Object> deps = (List<Object>) typeDependencies.get( type );
            if ( deps == null )
            {
                deps = new ArrayList<>(  );
                typeDependencies.put(type, deps);
            }
            deps.add( dependency );

            // Add as all sub-interfaces
            addInterfaces(type.getInterfaces(), dependency);
        }
    }
}
