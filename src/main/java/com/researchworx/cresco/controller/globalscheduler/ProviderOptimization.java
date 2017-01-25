package com.researchworx.cresco.controller.globalscheduler;

import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solver;
import org.chocosolver.solver.search.strategy.Search;
import org.chocosolver.solver.search.strategy.selectors.values.IntDomainMiddle;
import org.chocosolver.solver.search.strategy.selectors.variables.FirstFail;
import org.chocosolver.solver.search.strategy.selectors.variables.Smallest;
import org.chocosolver.solver.search.strategy.selectors.variables.VariableSelectorWithTies;
import org.chocosolver.solver.variables.BoolVar;
import org.chocosolver.solver.variables.IntVar;
import org.chocosolver.util.tools.ArrayUtils;

import java.util.*;

/**
 * Warehouse Location Problem
 * <p>
 *
 * @author Charles Prud'homme
 * @since 27/05/2016.
 */
public class ProviderOptimization {

    public Map<Integer, List<Integer>> opMap = null;

    public Map<Integer, List<Integer>> modelAndSolve(int W, int S, int[] K, int[][] P ,int cBound) {

        // A new model instance
        Model model = new Model("WarehouseLocation");

        // VARIABLES
        // a warehouse is either open or closed
        BoolVar[] open = model.boolVarArray("o", W);
        // which warehouse supplies a store
        IntVar[] supplier = model.intVarArray("supplier", S, 1, W, false);
        // supplying cost per store
        IntVar[] cost = model.intVarArray("cost", S, 0, cBound, true);
        // Total of all costs
        IntVar tot_cost = model.intVar("tot_cost", 0, (cBound * S), true);

        // CONSTRAINTS
        for (int j = 0; j < S; j++) {
            // a warehouse is 'open', if it supplies to a store
            model.element(model.intVar(1), open, supplier[j], 1).post();
            // Compute 'cost' for each store
            model.element(cost[j], P[j], supplier[j], 1).post();
        }
        for (int i = 0; i < W; i++) {
            // additional variable 'occ' is created on the fly
            // its domain includes the constraint on capacity
            IntVar occ = model.intVar("occur_" + i, 0, K[i], true);
            // for-loop starts at 0, warehouse index starts at 1
            // => we count occurrences of (i+1) in 'supplier'
            model.count(i+1, supplier, occ).post();
            // redundant link between 'occ' and 'open' for better propagation
            occ.ge(open[i]).post();
        }
        // Prepare the constraint that maintains 'tot_cost'
        int[] coeffs = new int[W + S];
        //Removed warehouse cost
        //Arrays.fill(coeffs, 0, W, C);
        Arrays.fill(coeffs, W, W + S, 1);
        // then post it
        model.scalar(ArrayUtils.append(open, cost), coeffs, "=", tot_cost).post();

        model.setObjective(false, tot_cost);
        Solver solver = model.getSolver();
        solver.setSearch(Search.intVarSearch(
                new VariableSelectorWithTies(
                        new FirstFail(model),
                        new Smallest()),
                new IntDomainMiddle(false),
                ArrayUtils.append(supplier, cost, open))
        );
        //solver.showShortStatistics();
        while(solver.solve()){
            buildReturn(model, open, W, supplier, S, tot_cost);
        }
        //prettyPrint2(model, open, W, supplier, S, tot_cost);
    return opMap;
    }


    public void modelAndSolve2() {
        // load parameters
        // number of warehouses
        int W = 5;
        // number of stores
        int S = 4;
        // maintenance cost
        //int C = 100;
        // capacity of each warehouse
        int[] K = new int[]{1, 2, 2, 2, 1};
        // matrix of supply costs, store x warehouse

        int[][] P = new int[][]{
                {0, 90, 90, 90, 90},
                {28, 0, 82, 83, 74},
                {74, 60, 0, 96, 70},
                {90, 90, 90, 90, 0}};

        /*
        int[][] P = new int[][]{
                {20, 24, 11, 25, 30},
                {28, 27, 82, 83, 74},
                {74, 97, 71, 96, 70},
                {2, 55, 73, 69, 61},
                {46, 96, 59, 83, 4},
                {42, 22, 29, 67, 59},
                {1, 5, 73, 59, 56},
                {10, 73, 13, 43, 96},
                {93, 35, 63, 85, 46},
                {47, 65, 55, 71, 95}};
         */
        // A new model instance
        Model model = new Model("WarehouseLocation");

        // VARIABLES
        // a warehouse is either open or closed
        BoolVar[] open = model.boolVarArray("o", W);
        // which warehouse supplies a store
        IntVar[] supplier = model.intVarArray("supplier", S, 1, W, false);
        // supplying cost per store
        IntVar[] cost = model.intVarArray("cost", S, 0, 96, true);
        // Total of all costs
        IntVar tot_cost = model.intVar("tot_cost", 0, 99999, true);

        // CONSTRAINTS
        for (int j = 0; j < S; j++) {
            // a warehouse is 'open', if it supplies to a store
            model.element(model.intVar(1), open, supplier[j], 1).post();
            // Compute 'cost' for each store
            model.element(cost[j], P[j], supplier[j], 1).post();
        }
        for (int i = 0; i < W; i++) {
            // additional variable 'occ' is created on the fly
            // its domain includes the constraint on capacity
            IntVar occ = model.intVar("occur_" + i, 0, K[i], true);
            // for-loop starts at 0, warehouse index starts at 1
            // => we count occurrences of (i+1) in 'supplier'
            model.count(i+1, supplier, occ).post();
            // redundant link between 'occ' and 'open' for better propagation
            occ.ge(open[i]).post();
        }
        // Prepare the constraint that maintains 'tot_cost'
        int[] coeffs = new int[W + S];
        //Removed warehouse cost
        //Arrays.fill(coeffs, 0, W, C);
        Arrays.fill(coeffs, W, W + S, 1);
        // then post it
        model.scalar(ArrayUtils.append(open, cost), coeffs, "=", tot_cost).post();

        model.setObjective(false, tot_cost);
        Solver solver = model.getSolver();
        solver.setSearch(Search.intVarSearch(
                new VariableSelectorWithTies(
                        new FirstFail(model),
                        new Smallest()),
                new IntDomainMiddle(false),
                ArrayUtils.append(supplier, cost, open))
        );
        solver.showShortStatistics();
        while(solver.solve()){
            prettyPrint(model, open, W, supplier, S, tot_cost);
        }

    }

    private void buildReturn(Model model, IntVar[] open, int W, IntVar[] supplier, int S, IntVar tot_cost) {
        opMap = new HashMap<>();

        //StringBuilder st = new StringBuilder();
        //st.append("Solution #").append(model.getSolver().getSolutionCount()).append("\n");
        for (int i = 0; i < W; i++) {
            if (open[i].getValue() > 0) {

                //st.append(String.format("\tWarehouse %d supplies customers : ", (i)));
                opMap.put(i,new ArrayList<Integer>());
                for (int j = 0; j < S; j++) {
                    if (supplier[j].getValue() == (i + 1)) {
                        opMap.get(i).add(j);
                   //     st.append(String.format("%d ", (j)));
                    }
                }
                //st.append("\n");
            }
        }
        //st.append("\tTotal C: ").append(tot_cost.getValue());
        //System.out.println(st.toString());
    }

    private void buildReturnDebug(Model model, IntVar[] open, int W, IntVar[] supplier, int S, IntVar tot_cost) {
        opMap = new HashMap<>();

        StringBuilder st = new StringBuilder();
        st.append("Solution #").append(model.getSolver().getSolutionCount()).append("\n");
        for (int i = 0; i < W; i++) {
            if (open[i].getValue() > 0) {

                st.append(String.format("\tWarehouse %d supplies customers : ", (i)));
                opMap.put(i,new ArrayList<Integer>());
                for (int j = 0; j < S; j++) {
                    if (supplier[j].getValue() == (i + 1)) {
                        opMap.get(i).add(j);
                        st.append(String.format("%d ", (j)));
                    }
                }
                st.append("\n");
            }
        }
        //st.append("\tTotal C: ").append(tot_cost.getValue());
        System.out.println(st.toString());
    }

    private void prettyPrint(Model model, IntVar[] open, int W, IntVar[] supplier, int S, IntVar tot_cost) {
        StringBuilder st = new StringBuilder();
        st.append("Solution #").append(model.getSolver().getSolutionCount()).append("\n");
        for (int i = 0; i < W; i++) {
            if (open[i].getValue() > 0) {
                st.append(String.format("\tWarehouse %d supplies customers : ", (i + 1)));
                for (int j = 0; j < S; j++) {
                    if (supplier[j].getValue() == (i + 1)) {
                        st.append(String.format("%d ", (j + 1)));
                    }
                }
                st.append("\n");
            }
        }
        st.append("\tTotal C: ").append(tot_cost.getValue());
        System.out.println(st.toString());
    }

}