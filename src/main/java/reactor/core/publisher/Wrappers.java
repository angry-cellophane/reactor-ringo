package reactor.core.publisher;

import org.reactivestreams.Publisher;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class Wrappers {

    public static Function<Publisher<Object>, Publisher<Object>> uberWrapper() {
        var context = new ConcurrentHashMap<>();

        return publisher -> {
            Publisher<Object> newPublisher;
            if (publisher instanceof Flux) {
                newPublisher = new FluxOnAssembly<>((Flux<Object>) publisher,
                        new FluxOnAssembly.AssemblySnapshot(null, Traces.callSiteSupplierFactory.get()));
            } else {
                newPublisher =  new MonoOnAssembly<>((Mono<Object>) publisher,
                        new FluxOnAssembly.AssemblySnapshot(null, Traces.callSiteSupplierFactory.get()));
            }
            return newPublisher;
        };
    }
}
