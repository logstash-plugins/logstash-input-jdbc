require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"

# This test requires: Firebird installed to Mac OSX, it uses the built-in example database `employee`

describe LogStash::Inputs::Jdbc, :integration => true do
  # This is a necessary change test-wide to guarantee that no local timezone
  # is picked up.  It could be arbitrarily set to any timezone, but then the test
  # would have to compensate differently.  That's why UTC is chosen.
  ENV["TZ"] = "Etc/UTC"
  let(:mixin_settings) do
    { "jdbc_user" => "SYSDBA", "jdbc_driver_class" => "org.firebirdsql.jdbc.FBDriver", "jdbc_driver_library" => "/elastic/tmp/jaybird-full-3.0.4.jar",
    "jdbc_connection_string" => "jdbc:firebirdsql://localhost:3050//Library/Frameworks/Firebird.framework/Versions/A/Resources/examples/empbuild/employee.fdb", "jdbc_password" => "masterkey"}
  end
  let(:settings) { {"statement" => "SELECT FIRST_NAME, LAST_NAME FROM EMPLOYEE WHERE EMP_NO > 144"} }
  let(:plugin) { LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings)) }
  let(:queue) { Queue.new }

  context "when passing no parameters" do
    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should retrieve params correctly from Event" do
      plugin.run(queue)
      event = queue.pop
      expect(event.get('first_name')).to eq("Mark")
      expect(event.get('last_name')).to eq("Guckenheimer")
    end
  end
end

