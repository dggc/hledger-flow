{-# LANGUAGE OverloadedStrings #-}

module Main where

import Test.HUnit
import Turtle
import Prelude hiding (FilePath)
import qualified Data.Map.Strict as Map
import qualified Control.Foldl as Fold

import Common

files = ["dir1/d1f1.journal", "dir1/d1f2.journal", "dir2/d2f1.journal", "dir2/d2f2.journal"] :: [FilePath]

expectedMap :: Map.Map FilePath [FilePath]
expectedMap = Map.fromList [("dir1.journal", ["dir1/d1f1.journal", "dir1/d1f2.journal"]),
                            ("dir2.journal", ["dir2/d2f1.journal", "dir2/d2f2.journal"])]

test1 = TestCase (assertEqual "takeLast" [3,5,7] (takeLast 3 [1,3,5,7]))

testGroupBy = TestCase (do
                           let grouped = groupValuesBy aggregateFileName files :: Map.Map FilePath [FilePath]
                           assertEqual "Group Files by Dir'" expectedMap grouped)

testGroupPairs = TestCase (do
                              let actual = groupPairs . pairBy aggregateFileName $ files
                              assertEqual "Sort And Group: Sorted" expectedMap actual)

tests = TestList [test1, testGroupBy, testGroupPairs]

main :: IO Counts
main = do
  counts <- runTestTT tests
  if (errors counts > 0 || failures counts > 0)
    then exit $ ExitFailure 1
    else return counts
