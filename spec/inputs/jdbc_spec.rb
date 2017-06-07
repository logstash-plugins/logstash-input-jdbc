# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"
require "jdbc/derby"
require "sequel"
require "sequel/adapters/jdbc"
require "timecop"
require "stud/temporary"
require "time"
require "date"

describe LogStash::Inputs::Jdbc do
  # This is a necessary change test-wide to guarantee that no local timezone
  # is picked up.  It could be arbitrarily set to any timezone, but then the test
  # would have to compensate differently.  That's why UTC is chosen.
  ENV["TZ"] = "Etc/UTC"
  let(:mixin_settings) do
    { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
      "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"}
  end
  let(:settings) { {} }
  let(:plugin) { LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings)) }
  let(:queue) { Queue.new }
  let (:db) do
    Sequel.connect(mixin_settings['jdbc_connection_string'], :user=> nil, :password=> nil)
  end

  before :each do
    Jdbc::Derby.load_driver
    db.create_table :test_table do
      DateTime :created_at
      Integer  :num
      String   :string
      DateTime :custom_time
    end
    db << "CREATE TABLE types_table (num INTEGER, string VARCHAR(255), started_at DATE, custom_time TIMESTAMP, ranking DECIMAL(16,6))"
  end

  after :each do
    db.drop_table(:test_table)
    db.drop_table(:types_table)
  end

  context "when registering and tearing down" do
    let(:settings) { {"statement" => "SELECT 1 as col1 FROM test_table"} }

    it "should register without raising exception" do
      expect { plugin.register }.to_not raise_error
      plugin.stop
    end

    it "should register with password set" do
      mixin_settings['jdbc_password'] = 'pass'
      expect { plugin.register }.to_not raise_error
      plugin.stop
    end

    it "should load all drivers when passing an array" do
      mixin_settings['jdbc_driver_library'] = '/foo/bar,/bar/foo'
      expect(plugin).to receive(:load_drivers).with(['/foo/bar', '/bar/foo'])
      plugin.register
      plugin.run(queue) # load when first run
      plugin.stop
    end

    it "should load all drivers when using a single value" do
      mixin_settings['jdbc_driver_library'] = '/foo/bar'
      expect(plugin).to receive(:load_drivers).with(['/foo/bar'])
      plugin.register
      plugin.run(queue) # load when first run
      plugin.stop
    end

    it "should stop without raising exception" do
      plugin.register
      expect { plugin.stop }.to_not raise_error
    end

    it_behaves_like "an interruptible input plugin" do
      let(:settings) do
        {
          "statement" => "SELECT 1 FROM test_table",
          "schedule" => "* * * * * UTC"
        }
      end
      let(:config) { mixin_settings.merge(settings) }
    end
  end

  context "when both jdbc_password and jdbc_password_filepath arguments are passed" do
    let(:statement) { "SELECT * from test_table" }
    let(:jdbc_password) { "secret" }
    let(:jdbc_password_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "jdbc_password_filepath" => jdbc_password_file_path,
                       "jdbc_password" => jdbc_password,
                       "statement" => statement } }

    it "should fail to register" do
      expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "when jdbc_password is passed in from a file" do
    let(:statement) { "SELECT * from test_table" }
    let(:jdbc_password) { "secret" }
    let(:jdbc_password_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "jdbc_password_filepath" => jdbc_password_file_path,
                       "statement" => statement } }

    before do
      File.write(jdbc_password_file_path, jdbc_password)
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should read in jdbc_password from file" do
      expect(plugin.jdbc_password.value).to eq(jdbc_password)
    end
  end


  context "when neither statement and statement_filepath arguments are passed" do
    it "should fail to register" do
      expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "when both statement and statement_filepath arguments are passed" do
    let(:statement) { "SELECT * from test_table" }
    let(:statement_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "statement_filepath" => statement_file_path, "statement" => statement } }

    it "should fail to register" do
      expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "when statement is passed in from a file" do
    let(:statement) { "SELECT * from test_table" }
    let(:statement_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "statement_filepath" => statement_file_path } }

    before do
      File.write(statement_file_path, statement)
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should read in statement from file" do
      expect(plugin.statement).to eq(statement)
    end
  end

  context "when passing parameters" do
    let(:settings) do
      {
        "statement" => "SELECT :num_param as num_param FROM SYSIBM.SYSDUMMY1",
        "parameters" => { "num_param" => 10}
      }
    end

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should retrieve params correctly from Event" do
      plugin.run(queue)
      expect(queue.pop.get('num_param')).to eq(settings['parameters']['num_param'])
    end
  end

  context "when scheduling" do
    let(:settings) { {"statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1", "schedule" => "* * * * * UTC"} }

    before do
      plugin.register
    end

    it "should properly schedule" do
      Timecop.travel(Time.new(2000))
      Timecop.scale(60)
      runner = Thread.new do
        plugin.run(queue)
      end
      sleep 3
      plugin.stop
      runner.kill
      runner.join
      expect(queue.size).to eq(2)
      Timecop.return
    end

  end

  context "when scheduling and previous runs are to be preserved" do
    let(:settings) do
      {
        "statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1",
        "schedule" => "* * * * * UTC",
        "last_run_metadata_path" => Stud::Temporary.pathname
      }
    end

    let(:last_run_time) { Time.at(1).utc }

    before do
      plugin.register
    end

    it "should flush previous run metadata per query execution" do
      Timecop.travel(Time.new(2000))
      Timecop.scale(60)
      runner = Thread.new do
        plugin.run(queue)
      end
      sleep 1
      for i in 0..1
        sleep 1
        updated_last_run = YAML.load(File.read(settings["last_run_metadata_path"]))
        expect(updated_last_run).to be > last_run_time
        last_run_time = updated_last_run
      end

      plugin.stop
    end

  end

  context "when iterating result-set via paging" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
        "jdbc_paging_enabled" => true,
        "jdbc_page_size" => 20
      }
    end

    let(:num_rows) { 1000 }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should fetch all rows" do
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc)
      end

      plugin.run(queue)

      expect(queue.size).to eq(num_rows)
    end

  end

  context "when fetching time data" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
      }
    end

    let(:num_rows) { 10 }

    before do
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should convert it to LogStash::Timestamp " do
      plugin.run(queue)
      event = queue.pop
      expect(event.get("custom_time")).to be_a(LogStash::Timestamp)
    end
  end

  context "when fetching time data with jdbc_default_timezone set" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true",
        "jdbc_default_timezone" => "America/Chicago"
      }
    end

    let(:settings) do
      {
        "statement" => "SELECT * from test_table WHERE custom_time > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "custom_time",
        "last_run_metadata_path" => Stud::Temporary.pathname
      }
    end

    let(:hour_range) { 10..20 }

    it "should convert the time to reflect the timezone " do
      last_run_value = Time.iso8601("2000-01-01T00:00:00.000Z")
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))

      hour_range.each do |i|
        db[:test_table].insert(:num => i, :custom_time => "2015-01-01 #{i}:00:00", :created_at => Time.now.utc)
      end

      plugin.register

      plugin.run(queue)
      expected = ["2015-01-01T16:00:00.000Z",
                  "2015-01-01T17:00:00.000Z",
                  "2015-01-01T18:00:00.000Z",
                  "2015-01-01T19:00:00.000Z",
                  "2015-01-01T20:00:00.000Z",
                  "2015-01-01T21:00:00.000Z",
                  "2015-01-01T22:00:00.000Z",
                  "2015-01-01T23:00:00.000Z",
                  "2015-01-02T00:00:00.000Z",
                  "2015-01-02T01:00:00.000Z",
                  "2015-01-02T02:00:00.000Z"].map { |i| Time.iso8601(i) }
      actual = queue.size.times.map { queue.pop.get("custom_time").time }
      expect(actual).to eq(expected)
      plugin.stop

      plugin.run(queue)
      expect(queue.size).to eq(0)
      db[:test_table].insert(:num => 11, :custom_time => "2015-01-01 11:00:00", :created_at => Time.now.utc)
      db[:test_table].insert(:num => 12, :custom_time => "2015-01-01 21:00:00", :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.size).to eq(1)
      event = queue.pop
      expect(event.get("num")).to eq(12)
      expect(event.get("custom_time").time).to eq(Time.iso8601("2015-01-02T03:00:00.000Z"))
      p settings
    end
  end

  context "when fetching time data without jdbc_default_timezone set" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
      }
    end

    let(:num_rows) { 1 }

    before do
      num_rows.times do
        db.run "INSERT INTO test_table (created_at, num, custom_time) VALUES (TIMESTAMP('2015-01-01 12:00:00'), 1, TIMESTAMP('2015-01-01 12:00:00'))"
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should not convert the time to reflect the timezone " do
      plugin.run(queue)
      event = queue.pop
      # With no timezone set, no change should occur
      expect(event.get("custom_time").time).to eq(Time.iso8601("2015-01-01T12:00:00Z"))
    end
  end

  context "when iteratively running plugin#run" do
    let(:settings) do
      {"statement" => "SELECT num, created_at FROM test_table WHERE created_at > :sql_last_value"}
    end

    let(:nums) { [10, 20, 30, 40, 50] }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully iterate table with respect to field values" do
      test_table = db[:test_table]

      plugin.run(queue)
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)

      actual_sum = 0
      until queue.empty? do
        actual_sum += queue.pop.get('num')
      end

      expect(actual_sum).to eq(nums.inject{|sum,x| sum + x })
    end
  end

  context "when iteratively running plugin#run with tracking_column" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at FROM test_table WHERE num > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "num",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully update sql_last_value" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(0)
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(20)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(50)
    end
  end

  context "when iteratively running plugin#run with timestamp tracking column with column value" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at, custom_time FROM test_table WHERE custom_time > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "custom_time",
        "tracking_column_type" => "timestamp",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }
    let(:times) {["2015-05-06 13:14:15","2015-05-07 13:14:15","2015-05-08 13:14:15","2015-05-09 13:14:15","2015-05-10 13:14:15"]}

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully update sql_last_value" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(Time.parse("1970-01-01 00:00:00.000000000 +0000"))
      test_table.insert(:num => nums[0], :created_at => Time.now.utc, :custom_time => times[0])
      test_table.insert(:num => nums[1], :created_at => Time.now.utc, :custom_time => times[1])
      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value").class).to eq(Time.parse(times[0]).class)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(Time.parse(times[1]))
      test_table.insert(:num => nums[2], :created_at => Time.now.utc, :custom_time => times[2])
      test_table.insert(:num => nums[3], :created_at => Time.now.utc, :custom_time => times[3])
      test_table.insert(:num => nums[4], :created_at => Time.now.utc, :custom_time => times[4])
      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(Time.parse(times[4]))
    end
  end

  context "when iteratively running plugin#run with tracking_column and stored metadata" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at FROM test_table WHERE num > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "num",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }
    let(:last_run_value) { 20 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully update sql_last_value and only add appropriate events" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(20)
      expect(queue.length).to eq(0) # Shouldn't grab anything here.
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(0) # Shouldn't grab anything here either.
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(20)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(3) # Only values greater than 20 should be grabbed.
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(50)
    end
  end

  context "when iteratively running plugin#run with BAD tracking_column and stored metadata" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at FROM test_table WHERE num > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "not_num",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }
    let(:last_run_value) { 20 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should send a warning and not update sql_last_value" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(20)
      expect(queue.length).to eq(0) # Shouldn't grab anything here.
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(0) # Shouldn't grab anything here either.
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(20)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(3) # Only values greater than 20 should be grabbed.
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(20)
      expect(plugin.instance_variable_get("@tracking_column_warning_sent")).to eq(true)
    end
  end

  context "when previous runs are to be respected upon successful query execution (by time)" do

    let(:settings) do
      { "statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_time) { Time.now.utc }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should respect last run metadata" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@sql_last_value")).to be > last_run_time
    end
  end

  context "when previous runs are to be respected upon successful query execution (by column)" do

    let(:settings) do
      { "statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1",
        "use_column_value" => true,
        "tracking_column" => "num_param",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_value) { 1 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "metadata should equal last_run_value" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@sql_last_value")).to eq(last_run_value)
    end
  end

  context "when previous runs are to be respected upon query failure (by time)" do
    let(:settings) do
      { "statement" => "SELECT col from non_existent_table",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_time) { Time.now.utc }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should not respect last run metadata" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@sql_last_value")).to eq(last_run_time)
    end
  end

  context "when previous runs are to be respected upon query failure (by column)" do
    let(:settings) do
      { "statement" => "SELECT col from non_existent_table",
        "use_column_value" => true,
        "tracking_column" => "num_param",
        "last_run_metadata_path" => Stud::Temporary.pathname
      }
    end

    let(:last_run_value) { 1 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "metadata should still reflect last value" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@sql_last_value")).to eq(last_run_value)
    end
  end

  context "when doing a clean run (by time)" do

    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "clean_run" => true
      }
    end

    let(:last_run_time) { Time.at(1).utc }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should ignore last run metadata if :clean_run set to true" do
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(Time.at(0).utc)
    end
  end

  context "when doing a clean run (by value)" do

    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "use_column_value" => true,
        "tracking_column" => "num_param",
        "clean_run" => true
      }
    end

    let(:last_run_value) { 1000 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should ignore last run metadata if :clean_run set to true" do
      expect(plugin.instance_variable_get("@sql_last_value")).to eq(0)
    end
  end


  context "when state is not to be persisted" do
    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "record_last_run" => false
      }
    end

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should not save state if :record_last_run is false" do
      expect(File).not_to exist(settings["last_run_metadata_path"])
    end
  end

  context "when setting fetch size" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
        "jdbc_fetch_size" => 1
      }
    end

    let(:num_rows) { 10 }

    before do
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should fetch all rows" do
      plugin.run(queue)
      expect(queue.size).to eq(num_rows)
    end
  end

  context "when driver is not found" do
    let(:settings) { { "statement" => "SELECT * FROM test_table" } }

    before do
      mixin_settings['jdbc_driver_class'] = "org.not.ExistsDriver"
    end

    it "should fail" do
      expect do
        plugin.register
        plugin.run(queue) # load when first run
      end.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "when timing out on connection" do
    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "jdbc_pool_timeout" => 0,
        "jdbc_connection_string" => 'mock://localhost:1527/db',
        "sequel_opts" => {
          "max_connections" => 1
        }
      }
    end

    it "should raise PoolTimeout error" do
      plugin.register
      plugin.run(queue)
      db = plugin.instance_variable_get(:@database)
      expect(db.pool.instance_variable_get(:@timeout)).to eq(0)
      expect(db.pool.instance_variable_get(:@max_size)).to eq(1)

      q, q1 = Queue.new, Queue.new
      t = Thread.new{db.pool.hold{|c| q1.push nil; q.pop}}
      q1.pop
      expect{db.pool.hold {|c|}}.to raise_error(Sequel::PoolTimeout)
      q.push nil
      t.join
    end

    it "should log error message" do
      allow(Sequel).to receive(:connect).and_raise(Sequel::PoolTimeout)
      expect(plugin.logger).to receive(:error).with("Failed to connect to database. 0 second timeout exceeded. Tried 1 times.")
      expect do
        plugin.register
        plugin.run(queue)
      end.to raise_error(Sequel::PoolTimeout)
    end
  end

  context "when using logging" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table", "sql_log_level" => "debug"
      }
    end

    let(:num_rows) { 5 }

    before do
      allow(plugin.logger).to receive(:debug?)
      num_rows.times do
        db[:test_table].insert(:num => 1)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should report the statements to logging" do
      expect(plugin.logger).to receive(:debug).once
      plugin.run(queue)
    end
  end

  describe "config option lowercase_column_names behaviour" do
    let(:settings) { { "statement" => "SELECT * FROM ttt" } }
    let(:events) { [] }

    before do
      db.create_table(:ttt) do
        Integer(:num)
        String(:somestring)
      end
      db[:ttt].insert(:num => 42, :somestring => "This is a string")
      plugin.register
    end

    after do
      plugin.stop
      db.drop_table(:ttt)
    end

    context "when lowercase_column_names is on (default)" do
      it "the field names are lower case" do
        plugin.run(events)
        expect(events.first.to_hash.keys.sort).to eq(
          ["@timestamp", "@version","num", "somestring"])
      end
    end

    context "when lowercase_column_names is off" do
      let(:settings) { { "statement" => "SELECT * FROM ttt", "lowercase_column_names" => false } }
      it "the field names are UPPER case (natural for Derby DB)" do
        plugin.run(events)
        expect(events.first.to_hash.keys.sort).to eq(
          ["@timestamp", "@version","NUM", "SOMESTRING"])
      end
    end
  end

  context "when specifying connection_retry_attempts" do
    let(:settings) { {"statement" => "SELECT 1 as col1 FROM test_table"} }

    it "should try to connect connection_retry_attempts times" do
      mixin_settings['connection_retry_attempts'] = 2
      mixin_settings['jdbc_pool_timeout'] = 0
      allow(Sequel).to receive(:connect).and_raise(Sequel::PoolTimeout)
      expect(plugin.logger).to receive(:error).with("Failed to connect to database. 0 second timeout exceeded. Trying again.")
      expect(plugin.logger).to receive(:error).with("Failed to connect to database. 0 second timeout exceeded. Tried 2 times.")
      expect do
        plugin.register
        plugin.run(queue)
      end.to raise_error(Sequel::PoolTimeout)
    end

    it "should not fail when passed a non-positive value" do
      mixin_settings['connection_retry_attempts'] = -2
      expect { plugin.register }.to_not raise_error
      plugin.stop
    end
  end

  context "when encoding of some columns need to be changed" do

    let(:settings) {{ "statement" => "SELECT * from test_table" }}
    let(:events)   { [] }
    let(:row) do
      {
        "column0" => "foo",
        "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
        "column2" => 3
      }
    end

    before(:each) do
      allow_any_instance_of(Sequel::JDBC::Derby::Dataset).to receive(:each).and_yield(row)
      plugin.register
    end

    after(:each) do
      plugin.stop
    end

    it "should not convert any column by default" do
      encoded_row = {
        "column0" => "foo",
        "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
        "column2" => 3
      }
      event = LogStash::Event.new(row)
      expect(LogStash::Event).to receive(:new) do |row|
        row.each do |k, v|
          next unless v.is_a?(String)
          expect(row[k].encoding).to eq(encoded_row[k].encoding)
        end

        event
      end
      plugin.run(events)
    end

    context "when all string columns should be encoded" do

      let(:settings) do
        {
          "statement" => "SELECT * from test_table",
          "charset" => "ISO-8859-1"
        }
      end

      let(:row) do
        {
          "column0" => "foo".force_encoding(Encoding::ISO_8859_1),
          "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
          "column2" => 3
        }
      end

      it "should transform all column string to UTF-8, default encoding" do
        encoded_row = {
          "column0" => "foo",
          "column1" => "bar",
          "column2" => 3
        }
        event = LogStash::Event.new(row)
        expect(LogStash::Event).to receive(:new) do |row|
          row.each do |k, v|
            next unless v.is_a?(String)
            expect(row[k].encoding).to eq(encoded_row[k].encoding)
          end
          
          event
        end
        plugin.run(events)
      end
    end

    context "when only an specific column should be converted" do

      let(:settings) do
        {
          "statement" => "SELECT * from test_table",
          "columns_charset" => { "column1" => "ISO-8859-1" }
        }
      end

      let(:row) do
        {
          "column0" => "foo",
          "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
          "column2" => 3,
          "column3" => "berlin".force_encoding(Encoding::ASCII_8BIT)
        }
      end

      it "should only convert the selected column" do
        encoded_row = {
          "column0" => "foo",
          "column1" => "bar",
          "column2" => 3,
          "column3" => "berlin".force_encoding(Encoding::ASCII_8BIT)
        }
        event = LogStash::Event.new(row)
        expect(LogStash::Event).to receive(:new) do |row|
          row.each do |k, v|
            next unless v.is_a?(String)
            expect(row[k].encoding).to eq(encoded_row[k].encoding)
          end

          event
        end
        plugin.run(events)
      end
    end
  end

  context "when fetching Various Typed data" do

    let(:settings) do
      {
      "statement" => "SELECT * from types_table"
      }
    end

    before do
      db << "INSERT INTO types_table (num, string, started_at, custom_time, ranking) VALUES (1, 'A test', '1999-12-31', '1999-12-31 23:59:59', 95.67)"

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should convert all columns to valid Event acceptable data types" do
      plugin.run(queue)
      event = queue.pop
      expect(event.get("num")).to eq(1)
      expect(event.get("string")).to eq("A test")
      expect(event.get("started_at")).to be_a(LogStash::Timestamp)
      expect(event.get("started_at").to_s).to eq("1999-12-31T00:00:00.000Z")
      expect(event.get("custom_time")).to be_a(LogStash::Timestamp)
      expect(event.get("custom_time").to_s).to eq("1999-12-31T23:59:59.000Z")
      expect(event.get("ranking").to_f).to eq(95.67)
    end
  end
end
