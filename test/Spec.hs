{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedLists #-}

module Main where

import Test.HUnit
import Turtle
import Prelude hiding (FilePath)

import qualified CSVImport.Unit
import qualified CSVImport.Integration

tests = TestList [CSVImport.Unit.tests, CSVImport.Integration.tests]

main :: IO Counts
main = do
  counts <- runTestTT tests
  if (errors counts > 0 || failures counts > 0)
    then exit $ ExitFailure 1
    else return counts
