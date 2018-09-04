package janusgraph.util.batchimport.unsafe.helps;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

/**
 * Used to create dynamic proxies that implement dependency interfaces. Each method should have no arguments
 * and return the type of the dependency desired. It will be mapped to a lookup in the provided {@link DependencyResolver}.
 * Methods may also use a {@link Supplier} type for deferred lookups.
 */
public class DependenciesProxy
{
    private DependenciesProxy()
    {
        throw new AssertionError(); // no instances
    }

    /**
     * Create a dynamic proxy that implements the given interface and backs invocation with lookups into the given
     * dependency resolver.
     *
     * @param dependencyResolver original resolver to proxy
     * @param dependenciesInterface interface to proxy
     * @param <T> type of the interface
     * @return a proxied {@link DependencyResolver} that will lookup dependencies in {@code dependencyResolver} based
     * on method names in the provided {@code dependenciesInterface}
     */
    public static <T> T dependencies( DependencyResolver dependencyResolver, Class<T> dependenciesInterface )
    {
        return dependenciesInterface.cast(
                Proxy.newProxyInstance( dependenciesInterface.getClassLoader(), new Class<?>[]{dependenciesInterface},
                        new ProxyHandler( dependencyResolver ) ) );
    }

    private static class ProxyHandler implements InvocationHandler
    {
        private DependencyResolver dependencyResolver;

        ProxyHandler( DependencyResolver dependencyResolver )
        {
            this.dependencyResolver = dependencyResolver;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            try
            {
                if ( method.getReturnType().equals( Supplier.class ) )
                {
                    return dependencyResolver.provideDependency(
                            (Class<?>) ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0] );
                }
                else
                {
                    return dependencyResolver.resolveDependency( method.getReturnType() );
                }
            }
            catch ( IllegalArgumentException e )
            {
                throw e;
            }
        }
    }
}
