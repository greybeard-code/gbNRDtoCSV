#region Using declarations
using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Xml.Linq;
using NinjaTrader.Cbi;
using NinjaTrader.Core;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.Gui.Tools;
#endregion

namespace NinjaTrader.Gui.NinjaScript
{
    public class gbNRDtoCSV : AddOnBase
    {
        private NTMenuItem menuItem;
        private NTMenuItem existingMenuItemInControlCenter;

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name = "gbNRDtoCSV";
                Description = "*.nrd to *.csv market replay files convertion";
            }
        }

        protected override void OnWindowCreated(Window window)
        {
            ControlCenter cc = window as ControlCenter;
            if (cc == null) return;

            existingMenuItemInControlCenter = cc.FindFirst("ControlCenterMenuItemTools") as NTMenuItem;
            if (existingMenuItemInControlCenter == null) return;

            menuItem = new NTMenuItem { Header = "NRD to CSV", Style = Application.Current.TryFindResource("MainMenuItem") as Style };
            existingMenuItemInControlCenter.Items.Add(menuItem);
            menuItem.Click += OnMenuItemClick;
        }

        protected override void OnWindowDestroyed(Window window)
        {
            if (menuItem != null && window is ControlCenter)
            {
                if (existingMenuItemInControlCenter != null && existingMenuItemInControlCenter.Items.Contains(menuItem))
                    existingMenuItemInControlCenter.Items.Remove(menuItem);
                menuItem.Click -= OnMenuItemClick;
                menuItem = null;
            }
        }

        private void OnMenuItemClick(object sender, RoutedEventArgs e)
        {
            Core.Globals.RandomDispatcher.BeginInvoke(new Action(() => new gbNRDtoCSVWindow().Show()));
        }
    }

    public class gbNRDtoCSVWindow : NTWindow, IWorkspacePersistence
    {
        private static readonly int PARALLEL_THREADS_COUNT = 4;

        private TextBox tbCsvRootDir;
        private TextBox tbInstrumentFilter;
        private Button bConvert;
        private TextBox tbOutput;
        private Label lProgress;
        private ProgressBar pbProgress;
        private ScrollViewer svInstruments;
        private StackPanel spInstruments;
        private HashSet<string> uncheckedInstruments;
        private int taskCount;
        private DateTime startTimestamp;
        private long completeFilesLength;
        private long totalFilesLength;
        private bool running = false;
        private bool canceling = false;

        public gbNRDtoCSVWindow()
        {
            Caption = "NRD to CSV";
            Width = 512;
            Height = 544;
            Content = BuildContent();
            Loaded += (o, e) =>
            {
                if (WorkspaceOptions == null)
                    WorkspaceOptions = new WorkspaceOptions("gbNRDtoCSV-" + Guid.NewGuid().ToString("N"), this);
                ScanInstruments();
            };
            Closing += (o, e) =>
            {
                if (bConvert != null)
                    bConvert.Click -= OnConvertButtonClick;
            };
        }

        protected override void OnClosed(EventArgs e)
        {
            if (running)
                canceling = true;
            base.OnClosed(e);
        }

        private DependencyObject BuildContent()
        {
            double margin = (double)FindResource("MarginBase");
            tbCsvRootDir = new TextBox()
            {
                Margin = new Thickness(margin, 0, margin, margin),
                Text = Path.Combine(Globals.UserDataDir, "db", "replay.csv"),
            };
            Label lCsvRootDir = new Label()
            {
                Foreground = FindResource("FontLabelBrush") as Brush,
                Margin = new Thickness(margin, 0, margin, 0),
                Content = "Root directory of converted CSV files:",
            };

            Button btnSelectAll = new Button() { Content = "All", Padding = new Thickness(6, 0, 6, 0), Margin = new Thickness(0, 0, 4, 0) };
            Button btnSelectNone = new Button() { Content = "None", Padding = new Thickness(6, 0, 6, 0) };
            btnSelectAll.Click += (s, ev) => { foreach (CheckBox cb in spInstruments.Children.OfType<CheckBox>()) cb.IsChecked = true; };
            btnSelectNone.Click += (s, ev) => { foreach (CheckBox cb in spInstruments.Children.OfType<CheckBox>()) cb.IsChecked = false; };
            StackPanel spButtons = new StackPanel() { Orientation = Orientation.Horizontal, VerticalAlignment = VerticalAlignment.Center };
            spButtons.Children.Add(btnSelectAll);
            spButtons.Children.Add(btnSelectNone);
            DockPanel dpInstrumentsHeader = new DockPanel() { Margin = new Thickness(margin, margin, margin, 0) };
            DockPanel.SetDock(spButtons, Dock.Right);
            dpInstrumentsHeader.Children.Add(spButtons);
            dpInstrumentsHeader.Children.Add(new Label()
            {
                Foreground = FindResource("FontLabelBrush") as Brush,
                Content = "Instruments to convert:",
                Padding = new Thickness(0),
                VerticalContentAlignment = VerticalAlignment.Center,
            });

            Button btnCheckMatching = new Button() { Content = "Check", Padding = new Thickness(6, 0, 6, 0), Margin = new Thickness(4, 0, 4, 0) };
            Button btnUncheckMatching = new Button() { Content = "Uncheck", Padding = new Thickness(6, 0, 6, 0) };
            btnCheckMatching.Click += (s, ev) => ApplyInstrumentFilter(true);
            btnUncheckMatching.Click += (s, ev) => ApplyInstrumentFilter(false);
            StackPanel spFilterButtons = new StackPanel() { Orientation = Orientation.Horizontal, VerticalAlignment = VerticalAlignment.Center };
            spFilterButtons.Children.Add(btnCheckMatching);
            spFilterButtons.Children.Add(btnUncheckMatching);
            tbInstrumentFilter = new TextBox()
            {
                VerticalContentAlignment = VerticalAlignment.Center,
                ToolTip = "Semicolon separated regular expressions matched against instrument names, e.g. \"MNQ\" or \"ES;NQ\" or \"^MNQ (03|06|09|12)-2[12]$\"",
            };
            DockPanel dpInstrumentsFilter = new DockPanel() { Margin = new Thickness(margin, margin, margin, 0) };
            DockPanel.SetDock(spFilterButtons, Dock.Right);
            dpInstrumentsFilter.Children.Add(spFilterButtons);
            dpInstrumentsFilter.Children.Add(tbInstrumentFilter);

            spInstruments = new StackPanel();
            spInstruments.Children.Add(new TextBlock()
            {
                Text = "Scanning...",
                Foreground = FindResource("FontLabelBrush") as Brush,
                Margin = new Thickness(4),
            });
            svInstruments = new ScrollViewer()
            {
                Height = 150,
                VerticalScrollBarVisibility = ScrollBarVisibility.Auto,
                Content = spInstruments,
                Margin = new Thickness(margin, 0, margin, margin),
            };

            bConvert = new Button() { Margin = new Thickness(margin), IsDefault = true, Content = "_Convert" };
            bConvert.Click += OnConvertButtonClick;
            tbOutput = new TextBox()
            {
                IsReadOnly = true,
                HorizontalScrollBarVisibility = ScrollBarVisibility.Auto,
                VerticalScrollBarVisibility = ScrollBarVisibility.Auto,
                Margin = new Thickness(margin),
            };
            pbProgress = new ProgressBar()
            {
                Height = 0,
            };
            lProgress = new Label()
            {
                Foreground = FindResource("FontLabelBrush") as Brush,
                Height = 0,
            };

            Grid grid = new Grid() { Background = new SolidColorBrush(Colors.Transparent) };
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition() { Height = new GridLength(1, GridUnitType.Star) });
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition() { Height = GridLength.Auto });
            Grid.SetRow(lCsvRootDir, 0);
            Grid.SetRow(tbCsvRootDir, 1);
            Grid.SetRow(dpInstrumentsHeader, 2);
            Grid.SetRow(dpInstrumentsFilter, 3);
            Grid.SetRow(svInstruments, 4);
            Grid.SetRow(bConvert, 5);
            Grid.SetRow(tbOutput, 6);
            Grid.SetRow(lProgress, 7);
            Grid.SetRow(pbProgress, 8);
            grid.Children.Add(lCsvRootDir);
            grid.Children.Add(tbCsvRootDir);
            grid.Children.Add(dpInstrumentsHeader);
            grid.Children.Add(dpInstrumentsFilter);
            grid.Children.Add(svInstruments);
            grid.Children.Add(bConvert);
            grid.Children.Add(tbOutput);
            grid.Children.Add(lProgress);
            grid.Children.Add(pbProgress);
            return grid;
        }

        private void ScanInstruments()
        {
            string nrdDir = Path.Combine(Globals.UserDataDir, "db", "replay");
            Globals.RandomDispatcher.InvokeAsync(new Action(() =>
            {
                string[] subDirs = null;
                if (Directory.Exists(nrdDir))
                    subDirs = Directory.GetDirectories(nrdDir);

                Dispatcher.InvokeAsync(() =>
                {
                    spInstruments.Children.Clear();
                    if (subDirs == null)
                    {
                        spInstruments.Children.Add(new TextBlock()
                        {
                            Text = "NRD replay directory not found.",
                            Foreground = FindResource("FontLabelBrush") as Brush,
                            Margin = new Thickness(4),
                        });
                        return;
                    }
                    if (subDirs.Length == 0)
                    {
                        spInstruments.Children.Add(new TextBlock()
                        {
                            Text = "No instruments found.",
                            Foreground = FindResource("FontLabelBrush") as Brush,
                            Margin = new Thickness(4),
                        });
                        return;
                    }
                    foreach (string subDir in subDirs)
                    {
                        string name = Path.GetFileName(subDir);
                        spInstruments.Children.Add(new CheckBox()
                        {
                            Content = name,
                            Tag = name,
                            IsChecked = true,
                            Margin = new Thickness(4, 2, 4, 2),
                        });
                    }
                    if (uncheckedInstruments != null)
                    {
                        foreach (CheckBox cb in spInstruments.Children.OfType<CheckBox>())
                            if (uncheckedInstruments.Contains((string)cb.Tag))
                                cb.IsChecked = false;
                        uncheckedInstruments = null;
                    }
                });
            }));
        }

        private void ApplyInstrumentFilter(bool check)
        {
            string pattern = tbInstrumentFilter.Text;
            if (string.IsNullOrWhiteSpace(pattern)) return;

            List<Regex> regexes;
            try
            {
                regexes = pattern.Split(';').Select(p => new Regex(p.Trim())).ToList();
            }
            catch (Exception error)
            {
                logout(string.Format("ERROR: Invalid regex \"{0}\": {1}", pattern, error.Message));
                return;
            }

            foreach (CheckBox cb in spInstruments.Children.OfType<CheckBox>())
                if (regexes.Any(r => r.IsMatch((string)cb.Tag)))
                    cb.IsChecked = check;
        }

        private void OnConvertButtonClick(object sender, RoutedEventArgs e)
        {
            if (tbOutput == null) return;
                logout("Run convertion...");

            if (running)
            {
                if (!canceling)
                {
                    canceling = true;
                    logout("Canceling convertion...");
                    bConvert.IsEnabled = false;
                    bConvert.Content = "Canceling...";
                }
                return;
            }

            tbOutput.Clear();

            string nrdDir = Path.Combine(Globals.UserDataDir, "db", "replay");
            string csvDir = tbCsvRootDir.Text;

            if (!Directory.Exists(nrdDir))
            {
                logout(string.Format("ERROR: The NRD root directory \"{0}\" not found", nrdDir));
                return;
            }

            HashSet<string> checkedInstruments = new HashSet<string>(
                spInstruments.Children.OfType<CheckBox>()
                    .Where(cb => cb.IsChecked == true)
                    .Select(cb => (string)cb.Tag));

            if (checkedInstruments.Count == 0)
            {
                logout("No instruments selected.");
                return;
            }

            string[] nrdSubDirs = Directory.GetDirectories(nrdDir)
                .Where(d => checkedInstruments.Contains(Path.GetFileName(d)))
                .ToArray();

            if (nrdSubDirs.Length == 0)
            {
                logout(string.Format("WARNING: No selected instruments found in \"{0}\"", nrdDir));
                return;
            }

            if (!Directory.Exists(csvDir))
            {
                try
                {
                    Directory.CreateDirectory(csvDir);
                }
                catch (Exception error)
                {
                    logout(string.Format("ERROR: Unable to create the CSV root directory \"{0}\": {1}", csvDir, error.ToString()));
                }
                return;
            }

            Globals.RandomDispatcher.InvokeAsync(new Action(() =>
            {
                completeFilesLength = 0;
                totalFilesLength = 0;
                List<DumpEntry> entries = new List<DumpEntry>();
                foreach (string subDir in nrdSubDirs)
                    ProceedDirectory(entries, nrdDir, subDir, csvDir);
                if (entries.Count == 0)
                {
                    logout("No *.nrd files found to convert");
                }
                else
                {
                    Globals.RandomDispatcher.InvokeAsync(new Action(() =>
                    {
                        logout(string.Format("Convert {0} files...", entries.Count));
                        run(entries.Count);
                        taskCount = PARALLEL_THREADS_COUNT;
                        for (int i = 0; i < taskCount; i++)
                            RunConversion(entries, i, taskCount);
                    }));
                }
            }));
        }

        private void ProceedDirectory(List<DumpEntry> entries, string nrdRoot, string nrdDir, string csvDir)
        {
            string[] fileEntries = Directory.GetFiles(nrdDir, "*.nrd");
            if (fileEntries.Length == 0)
            {
                logout(string.Format("WARNING: No *.nrd files found in \"{0}\" directory. Skipped", nrdDir));
                return;
            }

            foreach (string fileName in fileEntries)
            {
                string fullName = Path.GetFileName(Path.GetDirectoryName(fileName));
                string relativeName = fileName.Substring(nrdRoot.Length);

                Collection<Instrument> instruments = InstrumentList.GetInstruments(fullName);
                if (instruments.Count == 0)
                {
                    logout(string.Format("Unable to find an instrument named \"{0}\". Skipped", fullName));
                    continue;
                }
                else if (instruments.Count > 1)
                {
                    logout(string.Format("More than one instrument identified for name \"{0}\". Skipped", fullName));
                    continue;
                }
                Cbi.Instrument instrument = instruments[0];
                string name = Path.GetFileNameWithoutExtension(fileName);
                string csvFileName = string.Format("{0}.csv", Path.Combine(csvDir, instrument.FullName, name));
                if (File.Exists(csvFileName))
                {
                    logout(string.Format("Conversion \"{0}\" to \"{1}\" is done already. Skipped",
                        relativeName.Substring(1), csvFileName.Substring(csvDir.Length + 1)));
                    continue;
                }
                long nrdFileLength = new FileInfo(fileName).Length;
                totalFilesLength += nrdFileLength;
                entries.Add(new DumpEntry()
                {
                    NrdLength = nrdFileLength,
                    Instrument = instrument,
                    Date = new DateTime(
                        Convert.ToInt32(name.Substring(0, 4)),
                        Convert.ToInt32(name.Substring(4, 2)),
                        Convert.ToInt32(name.Substring(6, 2))),
                    CsvFileName = csvFileName,
                    FromName = relativeName.Substring(1),
                    ToName = csvFileName.Substring(csvDir.Length + 1),
                });
            }
        }

        private void RunConversion(List<DumpEntry> entries, int offset, int increment)
        {
            Globals.RandomDispatcher.InvokeAsync(new Action(() =>
            {
                for (int i = offset; i < entries.Count; i += increment)
                {
                    DumpEntry entry = entries[i];
                    ConvertNrd(entry);
                    Dispatcher.InvokeAsync(() =>
                    {
                        pbProgress.Value++;
                        completeFilesLength += entry.NrdLength;
                        string eta = "";
                        if (completeFilesLength > 0)
                        {
                            DateTime etaValue = new DateTime(
                                (long)((DateTime.Now.Ticks - startTimestamp.Ticks) * (totalFilesLength / (double)completeFilesLength - 1.0)));
                            eta = string.Format(" ETA: {0}:{1}", etaValue.Day - 1, etaValue.ToString("HH:mm:ss"));
                        }
                        lProgress.Content = string.Format("{0} of {1} files converted ({2} of {3}){4}",
                            pbProgress.Value, entries.Count, ToBytes(completeFilesLength), ToBytes(totalFilesLength), eta);
                    });
                    if (canceling) break;
                }
                if (--taskCount == 0)
                {
                    complete();
                    if (canceling)
                    {
                        logout("Conversion canceled");
                    }
                    else
                    {
                        logout("Conversion complete");
                    }
                }
            }));
        }

        private void ConvertNrd(DumpEntry entry)
        {
            logout(string.Format("Conversion \"{0}\" to \"{1}\"...", entry.FromName, entry.ToName));

            string csvFileDir = Path.GetDirectoryName(entry.CsvFileName);
            if (!Directory.Exists(csvFileDir))
            {
                try
                {
                    Directory.CreateDirectory(csvFileDir);
                }
                catch (Exception error)
                {
                    logout(string.Format("ERROR: Unable to create the CSV file directory \"{0}\": {1}",
                        csvFileDir, error.ToString()));
                    return;
                }
            }

            try
            {
                MarketReplay.DumpMarketDepth(entry.Instrument, entry.Date, entry.Date, entry.CsvFileName);
                logout(string.Format("Conversion \"{0}\" to \"{1}\" complete", entry.FromName, entry.ToName));
            }
            catch (Exception error)
            {
                logout(string.Format("ERROR: Conversion \"{0}\" to \"{1}\" failed: {2}",
                    entry.FromName, entry.ToName, error.ToString()));
            }
        }

        public void Restore(XDocument document, XElement element)
        {
            foreach (XElement elRoot in element.Elements())
            {
                if (elRoot.Name.LocalName.Contains("gbNRDtoCSV"))
                {
                    XElement elCsvRootDir = elRoot.Element("CsvRootDir");
                    if (elCsvRootDir != null)
                        tbCsvRootDir.Text = elCsvRootDir.Value;

                    XElement elUncheckedInstruments = elRoot.Element("UncheckedInstruments");
                    if (elUncheckedInstruments != null && !string.IsNullOrEmpty(elUncheckedInstruments.Value))
                        uncheckedInstruments = new HashSet<string>(elUncheckedInstruments.Value.Split(','));
                }
            }
        }

        public void Save(XDocument document, XElement element)
        {
            element.Elements().Where(el => el.Name.LocalName.Equals("gbNRDtoCSV")).Remove();
            XElement elRoot = new XElement("gbNRDtoCSV");
            XElement elCsvRootDir = new XElement("CsvRootDir", tbCsvRootDir.Text);
            string unchecked_ = string.Join(",", spInstruments.Children.OfType<CheckBox>()
                .Where(cb => cb.IsChecked == false)
                .Select(cb => (string)cb.Tag));
            XElement elUncheckedInstruments = new XElement("UncheckedInstruments", unchecked_);
            elRoot.Add(elCsvRootDir);
            elRoot.Add(elUncheckedInstruments);
            element.Add(elRoot);
        }

        public WorkspaceOptions WorkspaceOptions { get; set; }

        private void logout(string text)
        {
            Dispatcher.InvokeAsync(() =>
            {
                tbOutput.AppendText(text + Environment.NewLine);
                tbOutput.ScrollToEnd();
            });
        }

        private void run(int filesCount)
        {
            Dispatcher.InvokeAsync(() =>
            {
                running = true;
                canceling = false;
                bConvert.IsEnabled = true;
                bConvert.Content = "_Cancel";
                tbCsvRootDir.IsReadOnly = true;
                svInstruments.IsEnabled = false;
                double margin = (double)FindResource("MarginBase");
                lProgress.Margin = new Thickness(0);
                lProgress.Height = 24;
                pbProgress.Margin = new Thickness(margin);
                pbProgress.Height = 16;
                pbProgress.Minimum = 0;
                pbProgress.Maximum = filesCount;
                pbProgress.Value = 0;
                startTimestamp = DateTime.Now;
            });
        }

        private void complete()
        {
            Dispatcher.InvokeAsync(() =>
            {
                running = false;
                lProgress.Margin = new Thickness(0);
                lProgress.Height = 0;
                pbProgress.Margin = new Thickness(0);
                pbProgress.Height = 0;
                tbCsvRootDir.IsReadOnly = false;
                svInstruments.IsEnabled = true;
                bConvert.IsEnabled = true;
                bConvert.Content = "_Convert";
            });
        }

        public static string ToBytes(long bytes)
        {
            if (bytes < 1024) return string.Format("{0} B", bytes);
            double exp = (int)(Math.Log(bytes) / Math.Log(1024));
            return string.Format("{0:F1} {1}iB", bytes / Math.Pow(1024, exp), "KMGTPE"[(int)exp - 1]);
        }
    }

    public class DumpEntry
    {
        public long NrdLength { get; set; }
        public Cbi.Instrument Instrument { get; set; }
        public DateTime Date { get; set; }
        public string CsvFileName { get; set; }
        public string FromName { get; set; }
        public string ToName { get; set; }
    }
}
