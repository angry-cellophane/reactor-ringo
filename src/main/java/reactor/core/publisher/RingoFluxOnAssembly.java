package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

class RingoFluxOnAssembly<T> extends InternalFluxOperator<T, T> implements Fuseable, AssemblyOp {

    final FluxOnAssembly.AssemblySnapshot snapshotStack;

    /**
     * Create an assembly trace decorated as a {@link Flux}.
     */
    RingoFluxOnAssembly(Flux<? extends T> source, FluxOnAssembly.AssemblySnapshot snapshotStack) {
        super(source);
        this.snapshotStack = snapshotStack;
    }

    @Override
    public String stepName() {
        return snapshotStack.operatorAssemblyInformation();
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.ACTUAL_METADATA) return !snapshotStack.checkpointed;
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

        return super.scanUnsafe(key);
    }

    @Override
    public String toString() {
        return snapshotStack.operatorAssemblyInformation();
    }

    @Override
    @SuppressWarnings("unchecked")
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
        if(snapshotStack != null) {
            if (actual instanceof ConditionalSubscriber) {
                ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) actual;
                return new FluxOnAssembly.OnAssemblyConditionalSubscriber<>(cs, snapshotStack, source);
            }
            else {
                return new FluxOnAssembly.OnAssemblySubscriber<>(actual, snapshotStack, source);
            }
        }
        else {
            return actual;
        }
    }
}
