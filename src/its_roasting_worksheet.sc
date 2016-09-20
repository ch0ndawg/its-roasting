object its_roasting_worksheet {
  def simulation(/*sc*/ ncells : Int, nsteps : Int, nprocs: Int, leftX: Double = -10.0, rightX: Double = 10.0,
               sigma: Double = 3.0, ao: Double = 1.0, coeff: Double = 0.375) : Unit = {
    val dx = (rightX-leftX)/(ncells-1)
    
    def tempFromIdx(i: Int) : (Int, Double)= {
      val x = leftX + dx*i +dx/2.0
      (i, ao*math.exp(-x*x/(2.0*sigma*sigma)))
    }
    
    def interior(idx : (Int,Double) ) = {
    	 idx._1 > 0 && idx._1 < ncells-1
    }
    
    def stencil(item: (Int,Double) ) = {
      val (i,t) = item
      val newVals = Vector((i,t))
      val cVals = Vector((i, -2*coeff*t), (i-1,coeff*t), (i+1,coeff*t))
      newVals ++ (cVals filter interior)
    }
    
    val temp = (0 until ncells) map tempFromIdx
    
    for (step <- 0 until nsteps) {
    	println(step)
    	val stencilParts = temp.flatMap(stencil(_))
    	stencilParts.reduceByKey(_+_)
    }
  }
  simulation(100, 20, 4)
}