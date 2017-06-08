/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reforest.dataTree;

import org.junit.Assert;
import org.junit.Test;
import scala.collection.mutable.ListBuffer;

public class ForestTest {
    private final ForestIncremental<Double, Integer> fIncremental = new ForestIncremental<>(5, 10);
    private final ForestFull<Double, Integer> fFull = new ForestFull<>(5, 10);

    @Test
    public void init() {
        for(int i = 0 ; i < fIncremental.numTrees(); i++) {
            Assert.assertEquals(false, fIncremental.isActive(i));
            Assert.assertEquals(null, fIncremental.getTree()[i]);
        }

        for(int i = 0 ; i < fFull.numTrees(); i++) {
            Assert.assertEquals(true, fFull.isActive(i));
            Assert.assertNotEquals(null, fFull.getTree()[i]);
        }
    }

    @Test
    public void merge() {
        ForestIncremental<Double, Integer> fIncremental2 = new ForestIncremental<>(5, 10);
        ForestFull<Double, Integer> fFull2 = new ForestFull<>(5, 10);

        fIncremental.merge(fIncremental2);
        for(int i = 0 ; i < fIncremental.numTrees(); i++) {
            Assert.assertEquals(false, fIncremental.isActive(i));
            Assert.assertEquals(null, fIncremental.getTree()[i]);
        }

        Tree<Double, Integer> tree = new Tree(10);
        fIncremental2.add(2, tree);
        fIncremental.merge(fIncremental2);
        for(int i = 0 ; i < fIncremental.numTrees(); i++) {
            if(i == 2) {
                Assert.assertEquals(true, fIncremental.isActive(i));
                Assert.assertNotEquals(null, fIncremental.getTree()[i]);
            } else {
                Assert.assertEquals(false, fIncremental.isActive(i));
                Assert.assertEquals(null, fIncremental.getTree()[i]);
            }
        }

        fFull.merge(fFull2);
        for(int i = 0 ; i < fFull.numTrees(); i++) {
            Assert.assertEquals(true, fFull.isActive(i));
            Assert.assertNotEquals(null, fFull.getTree()[i]);
        }
        // TODO do more checks
    }

    @Test
    public void getLevel() {

        Tree<Double, Integer> tree = new Tree(10);
        fIncremental.add(0, tree);
        fFull.merge(fIncremental);

        Assert.assertEquals(0, fFull.getLevel(0, 0));
        Assert.assertEquals(1, fFull.getLevel(0, 1));
        Assert.assertEquals(1, fFull.getLevel(0,2));
        Assert.assertEquals(2, fFull.getLevel(0,3));
        Assert.assertEquals(2, fFull.getLevel(0,4));
        Assert.assertEquals(2, fFull.getLevel(0,5));
        Assert.assertEquals(2, fFull.getLevel(0,6));
        Assert.assertEquals(3, fFull.getLevel(0,7));
    }

    @Test
    public void getLeftChild() {

        Tree<Double, Integer> tree = new Tree(10);
        fIncremental.add(0, tree);
        fFull.merge(fIncremental);

        Assert.assertEquals(1, fFull.getLeftChild(0,0));
        Assert.assertEquals(3, fFull.getLeftChild(0,1));
        Assert.assertEquals(5, fFull.getLeftChild(0,2));
        Assert.assertEquals(7, fFull.getLeftChild(0,3));
        Assert.assertEquals(9, fFull.getLeftChild(0,4));
        Assert.assertEquals(11, fFull.getLeftChild(0,5));
    }

    @Test
    public void getRightChild() {

        Tree<Double, Integer> tree = new Tree(10);
        fIncremental.add(0, tree);
        fFull.merge(fIncremental);

        Assert.assertEquals(2, fFull.getRightChild(0,0));
        Assert.assertEquals(4, fFull.getRightChild(0,1));
        Assert.assertEquals(6, fFull.getRightChild(0,2));
        Assert.assertEquals(8, fFull.getRightChild(0,3));
        Assert.assertEquals(10, fFull.getRightChild(0,4));
        Assert.assertEquals(12, fFull.getRightChild(0,5));
    }

    @Test
    public void getParent() {

        Tree<Double, Integer> tree = new Tree(10);
        fIncremental.add(0, tree);
        fFull.merge(fIncremental);

        Assert.assertEquals(0, fFull.getParent(0,0));
        Assert.assertEquals(0, fFull.getParent(0,1));
        Assert.assertEquals(0, fFull.getParent(0,2));
        Assert.assertEquals(1, fFull.getParent(0,3));
        Assert.assertEquals(1, fFull.getParent(0,4));
        Assert.assertEquals(2, fFull.getParent(0,5));
    }

    @Test
    public void setIsLeaf() {

        Tree<Double, Integer> tree = new Tree(10);
        fIncremental.add(0, tree);
        fFull.merge(fIncremental);

        Assert.assertEquals(false, fFull.isLeaf(0,0));
        Assert.assertEquals(false, fFull.isLeaf(0,1));
        Assert.assertEquals(false, fFull.isLeaf(0,2));
        fFull.setLeaf(0,1);
        Assert.assertEquals(false, fFull.isLeaf(0,0));
        Assert.assertEquals(true, fFull.isLeaf(0,1));
        Assert.assertEquals(false, fFull.isLeaf(0,2));
    }

}
