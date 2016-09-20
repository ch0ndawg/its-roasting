import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ItsRoastingApp {
  val conf = new SparkConf().setMaster("local").setAppName("My App")
  val sc = new SparkContext(conf)
  
  def simulation(/*sc*/ ncells : Int, nsteps : Int, nprocs: Int, leftX: Double = -10.0, rightX: Double = 10.0,
               sigma: Double = 3.0, ao: Double = 1.0, coeff: Double = 0.375) : Unit = {
    val dx = (rightX-leftX)/(ncells-1) // determine spatial step size
    
    // REPLACE this with vector of values from the previous timestep
    def tempFromIdx(i: Int) : (Int, Double)= {
      val x = leftX + dx*i +dx/2.0
      (i, ao*math.exp(-x*x/(2.0*sigma*sigma)))
    }
    // possibly replace this with ghost cells
    def interior(idx : (Int,Double) ) = {
    	 idx._1 > 0 && idx._1 < ncells-1
    }
    
    // stencil takes the place of sparse matrix arithmetic
    def stencil(currentVal: (Int,Double), f : Vector[Double], k: Double ) = {
      val (i,u) = currentVal // replace with ((i,j),item) for production, or even ((i,j,k), item) if time permits
      val dtf = coeff*dx*dx/k * f(i) /* streaming data at timestep */ // coeff is k dt/dx^2
      //val currentVals = Vector((i,t)) // initialize new vals with only old vals
      
      // produce the variaous increments using the stencil (this is the "matrix multiplication")
      val incrementVals = Vector((i, -2*coeff*u), (i-1,coeff*u), (i+1,coeff*u), (i, dtf)) // also add in inhomogeneous term
      
      
      // get rid of the boundary points, and tack on the original item
      // this is perfectly set up for a reduceByKey
      (incrementVals filter interior) :+ currentVal
    }
    
    val temp = (0 until ncells) map tempFromIdx // replace with: previous timestep's u
    val tempParallel = sc.parallelize(temp)//partitionBy(new RangePartitioner(nprocs))
    val conductivity = 0.0 // need to set this globally
    for (step <- 0 until nsteps) {
    	println(step)
    	val newStreamingData = Vector[Double]() // read from Kafka stream here.
    	val stencilParts = tempParallel.flatMap(stencil(_,newStreamingData,conductivity))
    	val newTemp = stencilParts.reduceByKey(_+_)
    	// write out to database
    	// assign to oldTemp, or yield
    }
  }
  simulation(100, 20, 4)
}