require "large_object_store/version"
require "zlib"

module LargeObjectStore

  def self.wrap(store)
    RailsWrapper.new(store)
  end

  class CompressedEntry
    def initialize(value)
      @compressed_value = Zlib::Deflate.deflate(Marshal.dump(value))
    end

    def decompress
      Marshal.load(Zlib::Inflate.inflate(@compressed_value))
    end
  end

  class RailsWrapper
    attr_reader :store

    MAX_OBJECT_SIZE = 1024**2
    ITEM_HEADER_SIZE = 100

    def initialize(store)
      @store = store
    end

    def write(key, value, options = {})
      value = CompressedEntry.new(value) if options.delete(:compress)

      value = Marshal.dump(value)

      # calculate slice size; note that key length is a factor because
      # the key is stored on the same slab page as the value
      slice_size = MAX_OBJECT_SIZE - ITEM_HEADER_SIZE - key.bytesize

      # store number of pages
      pages = (value.size / slice_size.to_f).ceil

      if pages == 1
        @store.write("#{key}_0", value, options)
      else
        # store object
        page = 1
        loop do
          slice = value.slice!(0, slice_size)
          break if slice.size == 0

          return false unless @store.write("#{key}_#{page}", slice, options.merge(raw: true))
          page += 1
        end

        @store.write("#{key}_0", pages, options)
      end
    end

    def read(key)
      # read pages
      pages = @store.read("#{key}_0")
      return if pages.nil?

      data = if pages.is_a?(String)
        pages
      else
        # read sliced data
        keys = Array.new(pages).each_with_index.map{|_,i| "#{key}_#{i+1}" }
        slices = @store.read_multi(*keys).values
        return nil if slices.compact.size < pages
        slices.join("")
      end

      data = Marshal.load(data)

      if data.is_a? CompressedEntry
        data.decompress
      else
        data
      end
    end

    def fetch(key, options={})
      value = read(key)
      return value unless value.nil?
      value = yield
      write(key, value, options)
      value
    end

    def delete(key)
      @store.delete("#{key}_0")
    end
  end
end
