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
      DateTime :custom_time
    end
  end

  after :each do
    db.drop_table(:test_table)
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
      plugin.stop
    end

    it "should load all drivers when using a single value" do
      mixin_settings['jdbc_driver_library'] = '/foo/bar'
      expect(plugin).to receive(:load_drivers).with(['/foo/bar'])
      plugin.register
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
      expect(queue.pop['num_param']).to eq(settings['parameters']['num_param'])
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
      expect(event["custom_time"]).to be_a(LogStash::Timestamp)
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
        "statement" => "SELECT * from test_table",
      }
    end

    let(:num_rows) { 10 }

    before do
      stub_const('ENV', ENV.to_hash.merge('TZ' => 'UTC'))
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => "2015-01-01 12:00:00", :created_at => Time.now.utc)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should convert the time to reflect the timezone " do
      plugin.run(queue)
      event = queue.pop
      # This reflects a 6 hour time difference between UTC and America/Chicago
      expect(event["custom_time"].time).to eq(Time.iso8601("2015-01-01T18:00:00Z"))
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
      stub_const('ENV', ENV.to_hash.merge('TZ' => 'UTC'))
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
      expect(event["custom_time"].time).to eq(Time.iso8601("2015-01-01T12:00:00Z"))
    end
  end

  context "when iteratively running plugin#run" do
    let(:settings) do
      {"statement" => "SELECT num, created_at FROM test_table WHERE created_at > :sql_last_start"}
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
        actual_sum += queue.pop['num']
      end

      expect(actual_sum).to eq(nums.inject{|sum,x| sum + x })
    end
  end

  context "when previous runs are to be respected" do

    let(:settings) do
      { "statement" => "SELECT * FROM test_table",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_time) { Time.at(1).utc }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should respect last run metadata" do
      expect(plugin.instance_variable_get("@sql_last_start")).to eq(last_run_time)
    end
  end

  context "when doing a clean run" do

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
      expect(plugin.instance_variable_get("@sql_last_start")).to eq(Time.at(0).utc)
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
      expect { plugin.register }.to raise_error(LogStash::ConfigurationError)
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
      expect(plugin.logger).to receive(:error).with("Failed to connect to database. 0 second timeout exceeded.")
      expect { plugin.register }.to raise_error(Sequel::PoolTimeout)
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
      plugin.instance_variable_set("@logger", logger)
      allow(logger).to receive(:debug?)
      num_rows.times do
        db[:test_table].insert(:num => 1)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    let(:logger) { double("logger") }

    it "should report the staments to logging" do
      expect(logger).to receive(:debug).with(kind_of(String)).once
      plugin.run(queue)
    end
  end

end
