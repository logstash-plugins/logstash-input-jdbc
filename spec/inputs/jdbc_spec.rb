require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"
require "jdbc/derby"
require "timecop"
require "stud/temporary"


describe "jdbc" do
  let(:mixin_settings) { {"jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver", "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"} }

  before :each do
    Jdbc::Derby.load_driver
  end

  it "should register and tear down" do
    settings = {"statement" => "SELECT 1 as col1 FROM SYSIBM.SYSDUMMY1"}
    plugin = LogStash::Plugin.lookup("input", "jdbc").new(mixin_settings.merge(settings))
    expect { plugin.register }.to_not raise_error
    expect { plugin.teardown }.to_not raise_error
  end

  it "should retrieve params correctly from Event" do
    settings = {"statement" => "SELECT :num_param as num_param FROM SYSIBM.SYSDUMMY1", "parameters" => {"num_param" => 10} }
    plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
    plugin.register
    q = Queue.new
    plugin.run(q)
    insist { q.size } == 1
    insist { q.pop['num_param'] } == settings['parameters']['num_param']
    plugin.teardown
  end

  it "should properly schedule" do
    settings = {"statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1", "schedule" => "* * * * * UTC"}
    plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
    plugin.register
    q = Queue.new
    Timecop.travel(Time.new(2000))
    Timecop.scale(60)
    runner = Thread.new do
      plugin.run(q)
    end
    sleep 3
    plugin.teardown
    runner.kill
    runner.join
    insist { q.size } == 2
    Timecop.return
  end

  it "should appropriately page table" do
    require "sequel"
    require "sequel/adapters/jdbc"
    Jdbc::Derby.load_driver
    @database = Sequel.connect(mixin_settings['jdbc_connection_string'], :user=> nil, :password=> nil)
    @database.create_table :test_table do
      DateTime :created_at
      Integer :num
    end
    test_table = @database[:test_table]
    settings = {"statement" => "SELECT * from test_table",
                "jdbc_paging_enabled" => true, "jdbc_page_size" => 20}
    plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
    plugin.register
    q = Queue.new

    NUM_ROWS = 1000

    NUM_ROWS.times do
      test_table.insert(:num => 1, :created_at => Time.now.utc)
    end

    plugin.run(q)
    plugin.teardown

    insist { q.size } == NUM_ROWS

    @database.drop_table(:test_table)
  end

  it "should successfully iterate table with respect to field values" do
    require "sequel"
    require "sequel/adapters/jdbc"
    Jdbc::Derby.load_driver
    @database = Sequel.connect(mixin_settings['jdbc_connection_string'], :user=> nil, :password=> nil)
    @database.create_table :test_table do
      DateTime :created_at
      Integer :num
    end
    test_table = @database[:test_table]
    settings = {"statement" => "SELECT num, created_at FROM test_table WHERE created_at > :sql_last_start"}
    plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
    plugin.register
    q = Queue.new

    nums = [10, 20, 30, 40, 50]
    plugin.run(q)
    test_table.insert(:num => nums[0], :created_at => Time.now.utc)
    test_table.insert(:num => nums[1], :created_at => Time.now.utc)
    plugin.run(q)
    test_table.insert(:num => nums[2], :created_at => Time.now.utc)
    test_table.insert(:num => nums[3], :created_at => Time.now.utc)
    test_table.insert(:num => nums[4], :created_at => Time.now.utc)
    plugin.run(q)

    actual_sum = 0
    until q.empty? do
      actual_sum += q.pop['num']
    end

    plugin.teardown

    insist { actual_sum } == nums.inject{|sum,x| sum + x }

    @database.drop_table(:test_table)
  end

  context "persistence" do

    it "should respect last run metadata" do
      settings = {
        "statement" => "SELECT * FROM SYSIBM.SYSDUMMY1",
        "last_run_metadata_path" => Stud::Temporary.pathname
      }
      plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
      plugin.register
      q = Queue.new
      plugin.run(q)
      plugin.teardown

      insist { File.exists?(settings["last_run_metadata_path"]) }
      last_run = YAML.load(File.read(settings["last_run_metadata_path"]))

      plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
      plugin.register
      insist { plugin.instance_variable_get("@sql_last_start") } == last_run
      plugin.teardown
    end

    it "should ignore last run metadata if :clean_run set to true" do
      settings = {
        "statement" => "SELECT * FROM SYSIBM.SYSDUMMY1",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "clean_run" => true
      }

      File.open(settings["last_run_metadata_path"], "w") { |f| f.write(YAML.dump(Time.at(1).utc)) }

      plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
      plugin.register

      insist { plugin.instance_variable_get("@sql_last_start") } == Time.at(0).utc

      plugin.teardown
    end

    it "should not save state if :record_last_run is false" do
      settings = {
        "statement" => "SELECT * FROM SYSIBM.SYSDUMMY1",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "record_last_run" => false
      }

      plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
      plugin.register

      insist { File.exists?(settings["last_run_metadata_path"]) }

      plugin.teardown
    end
  end
end
