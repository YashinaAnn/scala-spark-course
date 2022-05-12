package marketing_analyzer.projection

class PurchaseProjectionBuilderUDAFTest extends AbstractProjectionTest {

  override protected val projectionBuilder: PurchaseProjectionBuilder = PurchaseProjectionUDAFBuilder
}