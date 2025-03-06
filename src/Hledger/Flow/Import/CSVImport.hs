{-# LANGUAGE OverloadedStrings #-}

module Hledger.Flow.Import.CSVImport
    ( importCSVs
    ) where

import qualified Turtle hiding (stdout, stderr, proc, procStrictWithErr)
import Turtle ((%), (</>), (<.>))
import Prelude hiding (putStrLn, writeFile)
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.List.NonEmpty as NonEmpty
import qualified Hledger.Flow.Types as FlowTypes
import Hledger.Flow.Import.Types
import Hledger.Flow.BaseDir (relativeToBase, effectiveRunDir)
import Hledger.Flow.Import.ImportHelpers
import Hledger.Flow.Import.ImportHelpersTurtle (extractImportDirs, writeIncludesUpTo, writeToplevelAllYearsInclude)
import Hledger.Flow.PathHelpers (TurtlePath, pathToTurtle)
import Hledger.Flow.DocHelpers (docURL)
import Hledger.Flow.Common
import Hledger.Flow.Logging
import Hledger.Flow.RuntimeOptions
import Control.Concurrent.STM
import Control.Monad
import Data.Maybe (fromMaybe, isNothing, mapMaybe)
import qualified System.PosixCompat.Files as PosixFiles
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Time.Clock (UTCTime(..))

type FileWasGenerated = Bool

importCSVs :: RuntimeOptions -> IO ()
importCSVs opts = Turtle.sh (
  do
    ch <- Turtle.liftIO newTChanIO
    logHandle <- Turtle.fork $ consoleChannelLoop ch
    Turtle.liftIO $ when (showOptions opts) (channelOutLn ch (Turtle.repr opts))
    Turtle.liftIO $ logVerbose opts ch "Starting import"
    (journals, diff) <- Turtle.time $ Turtle.liftIO $ importCSVs' opts ch
    let generatedJournals = filter snd journals
    Turtle.liftIO $ channelOutLn ch $ Turtle.format ("Imported " % Turtle.d % "/" % Turtle.d % " journals in " % Turtle.s) (length generatedJournals) (length journals) $ Turtle.repr diff
    Turtle.liftIO $ terminateChannelLoop ch
    Turtle.wait logHandle
  )

importCSVs' :: RuntimeOptions -> TChan FlowTypes.LogMessage -> IO [(TurtlePath, FileWasGenerated)]
importCSVs' opts ch = do
  let effectiveDir = effectiveRunDir (baseDir opts) (importRunDir opts)
  let startYearMsg = maybe " " (Turtle.format (" (for the year " % Turtle.d % " and onwards) ")) (importStartYear opts)
  channelOutLn ch $ Turtle.format ("Collecting input files" % Turtle.s % "from "%Turtle.fp) startYearMsg (pathToTurtle effectiveDir)
  (inputFiles, diff) <- Turtle.time $ findInputFiles (fromMaybe 0 $ importStartYear opts) effectiveDir

  let fileCount = length inputFiles
  if fileCount == 0 && isNothing (importStartYear opts) then
    do
      let msg = Turtle.format ("I couldn't find any input files underneath " % Turtle.fp
                        % "\n\nhledger-flow expects to find its input files in specifically\nnamed directories.\n\n" %
                        "Have a look at the documentation for a detailed explanation:\n" % Turtle.s) (pathToTurtle effectiveDir) (docURL "input-files")
      errExit 1 ch msg []
    else
    do
      channelOutLn ch $ Turtle.format ("Found " % Turtle.d % " input files" % Turtle.s % "in " % Turtle.s % ". Proceeding with import...") fileCount startYearMsg (Turtle.repr diff)
      let actions = map (extractAndImport opts ch . pathToTurtle) inputFiles :: [IO (TurtlePath, FileWasGenerated)]
      importedJournals <- parAwareActions opts actions
      (journalsOnDisk, journalFindTime) <- Turtle.time $ findJournalFiles effectiveDir
      (_, writeIncludeTime1) <- Turtle.time $ writeIncludesUpTo opts ch (pathToTurtle effectiveDir) $ fmap pathToTurtle journalsOnDisk
      (_, writeIncludeTime2) <- Turtle.time $ writeToplevelAllYearsInclude opts
      let includeGenTime = journalFindTime + writeIncludeTime1 + writeIncludeTime2
      channelOutLn ch $ Turtle.format ("Wrote include files for " % Turtle.d % " journals in " % Turtle.s) (length journalsOnDisk) (Turtle.repr includeGenTime)
      return importedJournals

extractAndImport :: RuntimeOptions -> TChan FlowTypes.LogMessage -> TurtlePath -> IO (TurtlePath, FileWasGenerated)
extractAndImport opts ch inputFile = do
  case extractImportDirs inputFile of
    Right importDirs -> importCSV opts ch importDirs inputFile
    Left errorMessage -> do
      errExit 1 ch errorMessage (inputFile, False)

getModificationTime :: TurtlePath -> IO UTCTime
getModificationTime path = do
    stat <- Turtle.stat path
    -- Convert modification time from FileStatus to UTCTime
    return $ posixSecondsToUTCTime $ realToFrac $ PosixFiles.modificationTime stat

-- | Recursively collect all files imported in rules files
collectImportedFiles :: RuntimeOptions -> TChan FlowTypes.LogMessage -> [TurtlePath] -> IO [TurtlePath]
collectImportedFiles opts ch initialFiles = do
  let loop seen [] = return (S.toList seen)
      loop seen (file:rest) = do
        if file `S.member` seen
          then loop seen rest  -- Skip if already processed to avoid cycles
          else do
            imports <- extractImports opts ch file
            let newSeen = S.insert file seen
            loop newSeen (imports ++ rest)

  loop S.empty initialFiles

-- | Extract import statements from a rules file
extractImports :: RuntimeOptions -> TChan FlowTypes.LogMessage -> TurtlePath -> IO [TurtlePath]
extractImports opts ch filePath = do
  logVerbose opts ch $ "Checking for includes in " <> Turtle.format Turtle.fp filePath
  fileContent <- T.readFile filePath
  let importLines = filter (T.isPrefixOf "include") $ T.lines fileContent
      importPaths = mapMaybe extractImportPath importLines
      -- Resolve relative paths based on the directory of the current file
      inlineBaseDir = Turtle.directory filePath
      resolvedPaths = map (\p -> if Turtle.isAbsolute p then p else inlineBaseDir </> p) importPaths

  -- Check if the imported files exist
  filterM (verboseTestFile opts ch) resolvedPaths

extractImportPath :: T.Text -> Maybe TurtlePath
extractImportPath line =
  case T.words line of
    ("include":path:_) -> Just (removeQuotes path)
    _ -> Nothing
  where
    removeQuotes s = case T.unpack s of
      '"':rest -> take (length rest - 1) rest  -- Remove quotes and return a String
      _ -> T.unpack s  -- Just convert to String directly

importCSV :: RuntimeOptions -> TChan FlowTypes.LogMessage -> ImportDirs -> TurtlePath -> IO (TurtlePath, FileWasGenerated)
importCSV opts ch importDirs srcFile = do
  let preprocessScript = accountDir importDirs </> "preprocess"
  let constructScript = accountDir importDirs </> "construct"
  let bankName = importDirLine bankDir importDirs
  let accountName = importDirLine accountDir importDirs
  let ownerName = importDirLine ownerDir importDirs
  (csvFile, preprocessHappened) <- preprocessIfNeeded opts ch preprocessScript bankName accountName ownerName srcFile
  let journalOut = changePathAndExtension "3-journal/" "journal" csvFile
  shouldImport <- if onlyNewFiles opts
    then do
      journalExists <- verboseTestFile opts ch journalOut
      (if not journalExists || preprocessHappened then return True else (do
            -- Check if journalOut is older than csvFile
            csvModTime <- getModificationTime csvFile
            journalModTime <- getModificationTime journalOut

            -- Check if any rules file candidate is newer than the journal file
            let rulesCandidates = rulesFileCandidates csvFile importDirs
            rulesFilesNewer <- if null rulesCandidates
                               then return False
                               else do
                                 rulesFilesExist <- filterM (verboseTestFile opts ch) rulesCandidates
                                 if null rulesFilesExist
                                   then return False
                                   else do
                                     -- Find all imported files recursively and check their modification times
                                     allRelevantFiles <- collectImportedFiles opts ch rulesFilesExist
                                     allFileModTimes <- mapM getModificationTime allRelevantFiles
                                     logVerbose opts ch $ Turtle.format ("Journal: " % Turtle.s) (T.pack srcFile)
                                     logVerbose opts ch $ Turtle.format ("Relevant files: " % Turtle.s) (T.pack $ show allRelevantFiles)
                                     return $ any (> journalModTime) allFileModTimes

            -- Import if csv is newer OR if any rules file or its imports are newer
            return (csvModTime > journalModTime || rulesFilesNewer)))
    else return True

  importFun <- if shouldImport
    then constructOrImport opts ch constructScript bankName accountName ownerName
    else do
      _ <- logNewFileSkip opts ch "import" journalOut
      return $ \_p1 _p2 -> return journalOut
  Turtle.mktree $ Turtle.directory journalOut
  out <- importFun csvFile journalOut
  return (out, shouldImport)

constructOrImport :: RuntimeOptions -> TChan FlowTypes.LogMessage -> TurtlePath -> Turtle.Line -> Turtle.Line -> Turtle.Line -> IO (TurtlePath -> TurtlePath -> IO TurtlePath)
constructOrImport opts ch constructScript bankName accountName ownerName = do
  constructScriptExists <- verboseTestFile opts ch constructScript
  if constructScriptExists
    then return $ customConstruct opts ch constructScript bankName accountName ownerName
    else return $ hledgerImport opts ch

-- | Check if the first file is older than any of the files in the given list.
-- Returns True if the target file is older than at least one of the source files.
-- If any of the files doesn't exist, returns False.
isFileOlder :: TurtlePath -> [TurtlePath] -> IO Bool
isFileOlder target sourceFiles = do
    targetExists <- Turtle.testfile target
    if not targetExists
        then return False
        else do
            targetTime <- getModificationTime target
            sourceTimesAndExists <- mapM getTimeIfExists sourceFiles
            -- Only consider files that exist
            let sourceTimes = [time | (exists, time) <- sourceTimesAndExists, exists]
            -- Target is older if any source is newer than target
            return $ any (> targetTime) sourceTimes
  where
    getTimeIfExists :: TurtlePath -> IO (Bool, UTCTime)
    getTimeIfExists path = do
        exists <- Turtle.testfile path
        if exists
            then do
                time <- getModificationTime path
                return (True, time)
            else return (False, UTCTime (toEnum 0) 0)

preprocessIfNeeded :: RuntimeOptions -> TChan FlowTypes.LogMessage -> TurtlePath -> Turtle.Line -> Turtle.Line -> Turtle.Line -> TurtlePath -> IO (TurtlePath, Bool)
preprocessIfNeeded opts ch script bank account owner src = do
  let csvOut = changePathAndExtension "2-preprocessed/" "csv" src
  scriptExists <- verboseTestFile opts ch script
  targetExists <- verboseTestFile opts ch csvOut
  shouldProceed <- if onlyNewFiles opts
    then do
      isTargetOlder <- if targetExists
                       then isFileOlder csvOut [script, src]
                       else return True
      return $ scriptExists && (not targetExists || isTargetOlder)
    else return scriptExists
  if shouldProceed
    then do
     out <- preprocess opts ch script bank account owner src csvOut
     return (out, True)
    else do
      _ <- logNewFileSkip opts ch "preprocess" csvOut
      if targetExists
        then return (csvOut, False)
        else return (src, False)

logNewFileSkip :: RuntimeOptions -> TChan FlowTypes.LogMessage -> T.Text -> TurtlePath -> IO ()
logNewFileSkip opts ch logIdentifier absTarget =
  Control.Monad.when (onlyNewFiles opts) $ do
   let relativeTarget = relativeToBase opts absTarget
   logVerbose opts ch
     $ Turtle.format
        ("Skipping " % Turtle.s
         % " - only creating new files and this output file already exists: '"
         % Turtle.fp
         % "'") logIdentifier relativeTarget

preprocess :: RuntimeOptions -> TChan FlowTypes.LogMessage -> TurtlePath -> Turtle.Line -> Turtle.Line -> Turtle.Line -> TurtlePath -> TurtlePath -> IO TurtlePath
preprocess opts ch script bank account owner src csvOut = do
  Turtle.mktree $ Turtle.directory csvOut
  let args = [Turtle.format Turtle.fp src, Turtle.format Turtle.fp csvOut, Turtle.lineToText bank, Turtle.lineToText account, Turtle.lineToText owner]
  let relScript = relativeToBase opts script
  let relSrc = relativeToBase opts src
  let cmdLabel = Turtle.format ("executing '" % Turtle.fp % "' on '" % Turtle.fp % "'") relScript relSrc
  _ <- timeAndExitOnErr opts ch cmdLabel channelOut channelErr (parAwareProc opts) (Turtle.format Turtle.fp script, args, Turtle.empty)
  return csvOut

hledgerImport :: RuntimeOptions -> TChan FlowTypes.LogMessage -> TurtlePath -> TurtlePath -> IO TurtlePath
hledgerImport opts ch csvSrc journalOut = do
  case extractImportDirs csvSrc of
    Right importDirs -> hledgerImport' opts ch importDirs csvSrc journalOut
    Left errorMessage -> do
      errExit 1 ch errorMessage csvSrc

hledgerImport' :: RuntimeOptions -> TChan FlowTypes.LogMessage -> ImportDirs -> TurtlePath -> TurtlePath -> IO TurtlePath
hledgerImport' opts ch importDirs csvSrc journalOut = do
  let candidates = rulesFileCandidates csvSrc importDirs

  maybeRulesFile <- firstExistingFile candidates
  let relCSV = relativeToBase opts csvSrc
  case maybeRulesFile of
    Just rf -> do
      let relRules = relativeToBase opts rf
      let hledger = Turtle.format Turtle.fp $ pathToTurtle . FlowTypes.hlPath . hledgerInfo $ opts :: T.Text
      let args = [
            "import", "--dry-run",
            "--file", Turtle.format Turtle.fp (directivesFile opts),
            Turtle.format Turtle.fp csvSrc,
            "--rules-file", Turtle.format Turtle.fp rf
            ]

      let cmdLabel = Turtle.format ("importing '" % Turtle.fp % "' using rules file '" % Turtle.fp % "'") relCSV relRules

      logVerbose opts ch $ Turtle.format ("Rules file candidates:\n" % Turtle.s)
          (T.intercalate "\n" $ map (Turtle.format Turtle.fp) candidates)
      ((_, stdOut, _), _) <- timeAndExitOnErr opts ch cmdLabel dummyLogger channelErr (parAwareProc opts) (hledger, args, Turtle.empty)
      let withoutDryRunText = T.unlines $ drop 2 $ T.lines stdOut
      _ <- T.writeFile journalOut withoutDryRunText
      return journalOut
    Nothing ->
      do
        let relativeCandidates = map (relativeToBase opts) candidates
        let candidatesTxt = T.intercalate "\n" $ map (Turtle.format Turtle.fp) relativeCandidates
        let msg = Turtle.format ("I couldn't find an hledger rules file while trying to import\n" % Turtle.fp
                          % "\n\nI will happily use the first rules file I can find from any one of these " % Turtle.d % " files:\n" % Turtle.s
                          % "\n\nHere is a bit of documentation about rules files that you may find helpful:\n" % Turtle.s)
                  relCSV (length candidates) candidatesTxt (docURL "rules-files")
        errExit 1 ch msg csvSrc

rulesFileCandidates :: TurtlePath -> ImportDirs -> [TurtlePath]
rulesFileCandidates csvSrc importDirs = statementSpecificRulesFiles csvSrc importDirs ++ generalRulesFiles importDirs

importDirLines :: (ImportDirs -> TurtlePath) -> ImportDirs -> [Turtle.Line]
importDirLines dirFun importDirs = NonEmpty.toList $ Turtle.textToLines $ Turtle.format Turtle.fp $ Turtle.dirname $ dirFun importDirs

importDirLine :: (ImportDirs -> TurtlePath) -> ImportDirs -> Turtle.Line
importDirLine dirFun importDirs = foldl (<>) "" $ importDirLines dirFun importDirs

generalRulesFiles :: ImportDirs -> [TurtlePath]
generalRulesFiles importDirs = do
  let bank = importDirLines bankDir importDirs
  let account = importDirLines accountDir importDirs
  let accountRulesFile = accountDir importDirs </> buildFilename (bank ++ account) "rules"

  let bankRulesFile = importDir importDirs </> buildFilename bank "rules"
  [accountRulesFile, bankRulesFile]

statementSpecificRulesFiles :: TurtlePath -> ImportDirs -> [TurtlePath]
statementSpecificRulesFiles csvSrc importDirs = do
  let srcBasename = T.unpack (Turtle.format Turtle.fp (Turtle.basename csvSrc))
  let srcLeafDir = T.unpack (Turtle.format Turtle.fp (Turtle.dirname csvSrc))
  let srcSpecificFilename = srcBasename <.> "rules"
  [accountDir importDirs </> "rules" </> srcLeafDir </> srcSpecificFilename]

customConstruct :: RuntimeOptions -> TChan FlowTypes.LogMessage -> TurtlePath -> Turtle.Line -> Turtle.Line -> Turtle.Line -> TurtlePath -> TurtlePath -> IO TurtlePath
customConstruct opts ch constructScript bank account owner csvSrc journalOut = do
  let script = Turtle.format Turtle.fp constructScript :: T.Text
  let relScript = relativeToBase opts constructScript
  let constructArgs = [Turtle.format Turtle.fp csvSrc, "-", Turtle.lineToText bank, Turtle.lineToText account, Turtle.lineToText owner]
  let constructCmdText = Turtle.format ("Running: " % Turtle.fp % " " % Turtle.s) relScript (showCmdArgs constructArgs)
  let stdLines = inprocWithErrFun (channelErrLn ch) (script, constructArgs, Turtle.empty)
  let hledger = Turtle.format Turtle.fp $ pathToTurtle . FlowTypes.hlPath . hledgerInfo $ opts :: T.Text
  let args = [
        "print", "--ignore-assertions",
        "--file", "-",
        "--output-file", Turtle.format Turtle.fp journalOut
        ]

  let relSrc = relativeToBase opts csvSrc
  let cmdLabel = Turtle.format ("executing '" % Turtle.fp % "' on '" % Turtle.fp % "'") relScript relSrc
  _ <- timeAndExitOnErr' opts ch cmdLabel [constructCmdText] channelOut channelErr (parAwareProc opts) (hledger, args, stdLines)
  return journalOut

